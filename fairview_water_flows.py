#Author: Cole Howell
#driver file for the fairview flow study

from flow_study import *


def main():

    g, b, h = config()
    try:
        separate_zone_study(g, b, h)
        visualize(g)

    except Exception:
        today = dt.datetime.today()
        filename = 'fairview_flow_study_errors.txt'
        path = f'C:/Users/chowell/WADC Dropbox/Cole Howell/PC/Documents/Flow Data/Hwy 96 Reports/{filename}'
        if os.path.exists(path):
            f = open(path, 'a')
            f.write(f'{today}\n')
            traceback.print_exc(file=f)
            f.write(f'\n')
        else:
            f = open(path, 'w')
            f.write(f'{today}\n')
            traceback.print_exc(file=f)
            f.write(f'\n')


if __name__ == '__main__':
    main()
