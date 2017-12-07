import os
import inspect

BASE_DIR = os.path.dirname(os.path.abspath(inspect.getfile(
                    inspect.currentframe()))) + '/'


visits = [410915, 410929, 410931, 410971, 410985, 410987,
          411021, 411035, 411037, 411055, 411069, 411071, 411255, 411269, 411271,
          411305, 411319, 411321, 411355, 411369, 411371, 411406, 411420, 411422,
          411456, 411470, 411472, 411657, 411671, 411673, 411707, 411721, 411724,
          411758, 411772, 411774, 411808, 411822, 411824, 411858, 411872, 411874,
          412060, 412074, 412076, 412250, 412264, 412266, 412307, 412321,
          412324, 412504, 412518, 412520, 412554, 412568, 412570, 412604,
          412618, 412620, 412654, 412668, 412670, 412704, 412718, 412720,
          413635, 413649, 413651, 413680, 413694, 413696,
          415314, 415328, 415330, 415364, 415378, 415380,
          419791, 419802, 419804, 421590, 421604, 421606]
# CCD 1 has image subtraction problems, CCDs 2 and 61 are missing
ccds = list(range(3, 61)) + [62]    # range does not support concatenation in Python 3

i=0
with open('demo_cmds.conf', 'w') as f:
    for visit in visits:
        for ccd in ccds:
            f.write('{} {}exec_demo_run.sh {} {}\n'.format(i, BASE_DIR, visit, ccd))
            i+=1
