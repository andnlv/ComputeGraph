from ComputeGraph import ComputeGraph


file_name1 = 'input1.txt'
file_name2 = 'input2.txt'


def test():
    graph1 = ComputeGraph('1')
    graph2 = ComputeGraph('2') \
        .join(graph1, keys = ('DepartmentID', 'DepartmentID'))
    graph2.run({'1' : file_name1, '2' : file_name2}, output='outut.txt')