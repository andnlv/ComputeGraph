import json


def get_key(row, key):
    if key == None:
        return []
    if isinstance(key, tuple):
        result = []
        for key in key:
            result.append(row[key])
        return result
    return row[key]


def reader(file_name):
    result = []
    for line in open(file_name).readlines():
        result.append(json.loads(line))
    return result


class ComputeGraph(object):
    def __init__(self, name=None):
        """
        :param name: will be used for directing input
        """
        self.result = None
        self._type = ''
        self._parent = None
        self.name = name

    def map(self, mapper):
        """
        Maps table with a mapper function
        :param mapper: generator that takes one row of the table.
        :return: Graph
        """
        result = ComputeGraph(self.name)
        result._parent = self
        result._type = '_map'
        result._mapper = mapper
        return result

    def sort(self, key):
        """
        Sort table by key columns in lexicographical order
        :param key: key of a table or tuple of keys
        :return: Graph
        """
        result = ComputeGraph(self.name)
        result._parent = self
        result._type = '_sort'
        result._key = key
        return result

    def fold(self, folder, begin_state, fold_name):
        """
        Apply a folder to the table. Put a result into one-row one-column table with fold_name key name
        :param folder: a folder function. Should take a current state as a first argument and next row as a second
        :param begin_state: begin state
        :param fold_name: name of a key
        :return: Graph
        """
        result = ComputeGraph(self.name)
        result._parent = self
        result._type = '_fold'
        result._folder = folder
        result._begin_state = begin_state
        result._fold_name = fold_name
        return result

    def reduce(self, reducer, key=None):
        """
        Apply reducer to the table
        :param reducer: reducer takes a list of rows with the same key
        :param key: key or tuple of keys of the table. If not given reducer will be applied to whole table
        :return: Graph
        """
        result = ComputeGraph(self.name)
        result._parent = self
        result._type = '_reduce'
        result._reducer = reducer
        result._key = key
        return result

    def join(self, on, keys=None):
        """
        Join graph with other one given in 'on' argument using 'inner' strategy
        :param on: Graph to be joined with
        :param keys: Join keys
        :return: Graph
        """
        result = ComputeGraph(self.name)
        result._type = '_join'
        result._parent = self
        result._on = on
        result._keys = keys
        return result

    def run(self, input, output=None):
        """
        Run the graph
        :param input: Name of the input file, if you have one input graph and a dictionary,
            mapping names of graph to the names of files. You should NOT open files before run.
        :param output: Name of output file. If not given, you can find the result in result field.
        """
        if self.result:
            if output == None:
                return self.result
            return
        if self._parent == None:
            if self.name == None:
                self.result = reader(input)
            else:
                self.result = reader(input[self.name])
            return

        self._parent.run(input)
        if self._type == '_join':
            self._on.run(input)
        getattr(self, self._type)()
        if output:
            with open(output, 'w') as f:
                for row in self.result:
                    json.dump(row, f)
                    f.write('\n')

    def _map(self):
        self.result = []
        for row in self._parent.result:
            for new_row in self._mapper(row):
                self.result.append(new_row)
        del self._parent.result

    def _sort(self):
        self.result = sorted(self._parent.result, key=lambda record: get_key(record, self._key))

    def _fold(self):
        self.result = self._begin_state
        for record in self._parent.result:
            self.result = self._folder(self.result, record)
        self.result = [{self._fold_name: self.result}]

    def _reduce(self):
        begin = 0
        end = 1
        previous_key = get_key(self._parent.result[0], self._key)
        self.result = []
        for i in range(1, len(self._parent.result)):
            current_key = get_key(self._parent.result[i], self._key)
            if current_key == previous_key:
                end += 1
            elif current_key > previous_key:
                for new_row in self._reducer(self._parent.result[begin: end]):
                    self.result.append(new_row)
                begin = end
                end += 1
            else:
                raise ValueError("Table should be sorted before being reduced")
            previous_key = current_key
        for new_row in self._reducer(self._parent.result[begin: end]):
            self.result.append(new_row)

    def _join(self):
        self.result = []
        for row1 in self._parent.result:
            for row2 in self._on.result:
                if self._keys == None or get_key(row1, self._keys[0]) == get_key(row2, self._keys[1]):
                    self.result.append(dict(**row1))
                    for key, value in row2.items():
                        if self._keys == None or \
                                (isinstance(self._keys[1], tuple) and key not in self._keys[1]) or \
                                key != self._keys[1]:
                            if key in self.result[-1]:
                                self.result[-1][self.name + key] = value
                            else:
                                self.result[-1][key] = value
        del self._parent.result
