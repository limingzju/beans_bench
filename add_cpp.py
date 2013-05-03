import os
import sys

def Add(fname):
    header = """\n#ifdef __cplusplus
extern "C" {
#endif\n"""

    footer = """\n#ifdef __cplusplus
}
#endif\n"""

    f = open(fname)
    lines = f.read();
    f.close()

    f = open(fname, 'w')
    f.write(header)
    f.write(lines)
    f.write(footer)
    f.close()

if __name__ == '__main__':
    for i in range(1, len(sys.argv)):
        Add(sys.argv[i])
