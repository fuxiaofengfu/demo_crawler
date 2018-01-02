#-*- encoding: utf-8 -*-
'''
Created on 2017/7/1 13:49
Copyright (c) 2017/7/1, 海牛学院版权所有.
@author: 青牛
'''
import os
import content

class FileUtil:
    
    def create_path(self, path):
        """create paths
        """
        if not os.path.exists(path):
            os.makedirs(path)

    def write_file_line_pattern(self, path, dataList, pattern='w'):
        """overwrite 'dataList' into path of file
        """
        f = file(path, pattern)
        for dat in dataList:
            line = dat.__str__() + content._SEQ2
            f.write(line)
        f.close()

    def write_file_line(self, path, dataList):
        """overwrite 'dataList' into path of file
        """
        self.write_file_line_pattern(path,dataList)

    def write_file_content_pattern(self, path, Content, pattern='w'):
        """overwrite 'dataList' into path of file
        """
        f = file(path, pattern)
        f.write(Content)
        f.close()

    def write_file_content(self, path, Content):
        """overwrite 'dataList' into path of file
        """
        self.write_file_content_pattern(path,Content)

    def read_file(self,path):
        list = []
        f = file(path)
        while True:
            line = f.readline()
            if len(line) == 0:
                break
            list.append(line.replace('\n','').replace('\r\n',''))
        f.close()
        return list