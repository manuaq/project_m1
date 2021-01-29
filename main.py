import argparse
from p_acquisition.m_acquisition import acquire



def argument_parser():
    """
    parse arguments to script
    """

    parser = argparse.ArgumentParser(
        description='pass table type and scope by selecting specific country vs. all countries...')

    # arguments here!
    parser.add_argument("-p", "--path", nargs='?', const='/Users/manuelaquino/Bootcamp/project_m1/data/raw/raw_data_project_m1.db', help='Specify database file path', type=str)
    parser.add_argument("-t", "--table", help='Specify Table Type', type=str)
    parser.add_argument("-s", "--sample", help='Specify country sample or show all Type', type=str)
    # arguments here!

    args = parser.parse_args()

    return args


def main(arguments):
    print('Starting process...')
    path = arguments.path
    print(path, ', As no other specific file path was given.', 'Opening: "raw_data_project_m1.db"')
    print(arguments.table)
    print(arguments.sample)

    if path == None:
        acquire('/Users/manuelaquino/Bootcamp/project_m1/data/raw/raw_data_project_m1.db')
    else:
        acquire(path=path)

    print('Process finished!')


if __name__ == '__main__':
    arguments = argument_parser()
    main(arguments)
