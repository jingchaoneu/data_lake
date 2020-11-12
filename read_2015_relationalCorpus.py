#coding = utf-8

import jieba
import json
import chardet
import os

class Read_2015_relationalCorpus:
    def __init__(self, PathObj):
        self.Path_in_Record = PathObj

    def read_json_wikiTable(self, path):
        with open(path, "rb") as file:
            for line in file:
                print(line)

            # for file in range(0, len(files)):
            #     stepOver = 1
            #     while stepOver < sampleStep:
            #         stepOver+=1
            #         continue

    # 只读取WDC一个文件，每行是一个json，返回json list
    def read_json_webTable(self, path, sampleStep=0):
        i = 1
        JsonList = []
        with open(path, "rb") as file:
            stepCount = 0

            for line in file:

                # 跳步采样, 步长=sampleStep
                # print(line)
                if stepCount < sampleStep:
                    stepCount += 1
                    continue
                stepCount = 0
                # print("read line", line)

                '''
                if i < 1630:
                    i += 1
                    continue
                '''

                encode = chardet.detect(line)["encoding"]

                if encode == "GB2312":
                    encode = "GBK"

                x = chardet.detect(line)

                # print(i)
                try:
                    line = line.decode(encode).strip()
                    # print(x)
                except:
                    line = line.decode("unicode_escape")
                    print("unicode_escape")

                # print(line)

                try:
                    line = json.loads(line)
                    # print(line)
                except:
                    print("error, transform failed(json.loads())")
                JsonList.append(line)
                i += 1
        print("read ", i, "json")
        return JsonList

    ########################## analysis dataset
    def checkKeyWord(self, keyWordsList, seg_list):
        for key in keyWordsList:
            if key in seg_list:
                return True
        return False

    def seekTables(self, keyWordsList):
        PathOFData = self.Path_in_Record.path_2015RelathionCorpus
        # r"D:\data set\WebTable\webtable 2015-07 relationalCorpus"
        for root, dirs, files in os.walk(PathOFData):
            # print("root: ", root) #当前目录路径
            # print("dirs", dirs) #当前路径下所有子目录
            # print("files", files) #当前路径下所有非目录子文件
            pass

            for file in files:
                path = root + "\\" + file
                print("path is :",path)
                JsonList = self.read_json_webTable(path=path)
                count = 0

                for json in JsonList:
                    count+=1
                    # print(type(json), count, "th table ", json)
                    try:
                        seg_list = jieba.cut(json["pageTitle"])
                    except:
                        print("error: jieba.cut(json[])")

                    if self.checkKeyWord(keyWordsList=keyWordsList, seg_list=seg_list):
                    # if "Restaurants" in seg_list or "restaurants" in seg_list or "restaurant" in seg_list or "Restaurant" in seg_list:

                        #         if "CricketArchive" in seg_list:
                        #         if "home" in seg_list or "Home" in seg_list:

                        print("\n*************************** ", count, "th table ", "****************************")
                        print("pageTitle：", json["pageTitle"])
                        print(json["url"], "\n")
                        print(json)
                        table = json["relation"]

                        print("原始table")
                        for row in table:
                            print(row)

                        print("\n转置table")
                        tableZip = list(zip(*table))
                        for row in tableZip:
                            print(row)
        print(count)

    def logOutliers_Statistics_table_numRowColumn(self, pair, json):
        pathWrite = self.Path_in_Record.path_2015RelathionCorpus_logOutliers
        # pathWrite = r"D:\data set\WebTable\webtable 2015-07 relationalCorpus log\logOutliers"

        with open(pathWrite, 'a') as f:
            print(pair, "outlier", json)

            f.write(str(pair))
            f.write(" ")
            try:
                f.write(str(json))
            except:
                print("write error")

            f.write('\n')

    def logCount_Statistics_table_numRowColumn(self, sum_numberOfElementIn_Row, sum_numberOfElementIn_Column, sumTable, distributed):
        pathWrite = self.Path_in_Record.path_2015RelathionCorpus_logStatistic
        # pathWrite = r"D:\data set\WebTable\webtable 2015-07 relationalCorpus log\logStatistic"
        with open(pathWrite, 'a') as f:
            print("sum_numberOfElementIn_Row", sum_numberOfElementIn_Row, " sum_numberOfElementIn_Column", sum_numberOfElementIn_Column, " sumTable", (sumTable-1))
            print("ave: numberOfElementIn_Row", sum_numberOfElementIn_Row/sumTable, " numberOfElementIn_Column", sum_numberOfElementIn_Column/sumTable)
            print("distributed: ", distributed)

            f.write(str(sum_numberOfElementIn_Row)+" "+str(sum_numberOfElementIn_Column)+" "+str(sumTable))
            f.write('\n')
            f.write(str(distributed))
            f.write('\n')

    # 分析结果保存在文件中
    # 列异常长的数据保存路径："D:\data set\WebTable\webtable 2015-07 relationalCorpus log\logOutliers"
    # 统计，分布保存文件："D:\data set\WebTable\webtable 2015-07 relationalCorpus log\logStatistic"
    def Statistics_table_numRowColumn(self, sampleStep=50, outlier=100):
        distributed = {"under10":118131, "under20":35321, "under30":12880, "under100":11422,"under200":2649,"under300":617,"under400":112,"under500":104,"upper500":241}

        sum_numberOfElementIn_Row = 940345
        sum_numberOfElementIn_Column = 2698548
        sumTable = 181535

        PathOFData = self.Path_in_Record.path_2015RelathionCorpus
        # r"D:\data set\WebTable\webtable 2015-07 relationalCorpus"
        for root, dirs, files in os.walk(PathOFData):
            # print("root: ", root) #当前目录路径
            # print("dirs", dirs) #当前路径下所有子目录
            # print("files", files) #当前路径下所有非目录子文件
            pass

            for file in files:
                path = root + "\\" + file
                print("\npath is :",path)
                JsonList = self.read_json_webTable(path=path, sampleStep=sampleStep)

                sumTable += len(JsonList)

                for json in JsonList:
                    # print(json)
                    try:
                        table = json["relation"]    # 转置之后才是table对应的column和row
                    except:
                        print("error:", json)

                    numberOfElementIn_Row = len(table)
                    numberOfElementIn_Column = len(table[0])

                    if numberOfElementIn_Column < 10:
                        distributed["under10"]+=1
                    elif numberOfElementIn_Column < 20:
                        distributed["under20"]+=1
                    elif numberOfElementIn_Column < 30:
                        distributed["under30"]+=1
                    elif numberOfElementIn_Column < 100:
                        distributed["under100"]+=1
                    elif numberOfElementIn_Column < 200:
                        distributed["under200"]+=1
                    elif numberOfElementIn_Column < 300:
                        distributed["under300"]+=1
                    elif numberOfElementIn_Column < 400:
                        distributed["under400"]+=1
                    elif numberOfElementIn_Column < 500:
                        distributed["under500"]+=1
                    else:
                        distributed["upper500"]+=1


                    sum_numberOfElementIn_Row += numberOfElementIn_Row
                    sum_numberOfElementIn_Column += numberOfElementIn_Column

                    pair=[numberOfElementIn_Row, numberOfElementIn_Column]
                    if numberOfElementIn_Row > outlier or numberOfElementIn_Column > outlier:
                        self.logOutliers_Statistics_table_numRowColumn(pair=pair, json=table)
                    # print(pair)
                self.logCount_Statistics_table_numRowColumn(sum_numberOfElementIn_Row=sum_numberOfElementIn_Row,
                                                            sum_numberOfElementIn_Column=sum_numberOfElementIn_Column,
                                                            sumTable=sumTable,
                                                            distributed=distributed)


