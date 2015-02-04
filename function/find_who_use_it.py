#!/usr/local/bin/python
#encoding:utf8
# 查看SVN路径下文件是否含有我要找的内容，比如说哪个文件用到了具体的表，就看脚本中是否有这个表就可以了

import sys
import os
import re

SVN_PATH = "/Users/MLS/SVN_work/galaxy/galaxy/trunk/libra"

# 要找的内容
wana_name = ""


def main():
    print "    function: main"
    for i in os.walk(SVN_PATH):
        for file in i[2]:
            file_name = "%s/%s" % (i[0],file)
            # print file_name
            f = open(file_name,'r')
            for line in f.readlines():
                # print line
                if re.search(wana_name,line):
                    print file_name
                    break
            f.close()


if __name__ == "__main__":
    # 启动方法  python find_who_use_it.py name
    # 其中 name 是想要在文件中找的内容

    # print len(sys.argv)
    if len(sys.argv) != 2:
        sys.exit(1)
    wana_name = sys.argv[1]

    main()
