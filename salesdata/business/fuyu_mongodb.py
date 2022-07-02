
from sql_setting import *
from pymongo import MongoClient,ASCENDING
import pandas as pd
import numpy as np
client = MongoClient(host='localhost', port=27017)


class dataChange:
    def __init__(self):
        self.db = client.fuyu
        self.fuyu = self.db.fuyu
        self.business = self.db.business
        self.spbu = self.db.spbu
        self.id = 0
        self.log = open("erro.txt",mode="a+",encoding="utf-8")
        self.rt = open("未查询到.txt",mode="a+",encoding="utf-8")
    def fuyuQuery(self):
        fuyu = self.db.fuyu

        i = 0
        while True:
            sql = "SELECT * FROM result ORDER BY `index` asc limit {},10000".format(i*10000)
            ress = cursor.execute(sql)
            res = [dict(zip(res.keys(),res)) for res in ress]
            i += 1
            if res:
                result = fuyu.insert_many(res)
            else:
                break
        business = self.db.business
        j = 0
        while True:
            sql = "SELECT * FROM business_relationship ORDER BY `index` asc limit {},10000".format(j*10000)
            ress = cursor.execute(sql)
            res = [dict(zip(res.keys(), res)) for res in ress]
            if res:
                j += 1
                result = business.insert_many(res)
            else:
                break
    def data_query(self):
        f = 0
        while True:
            result = self.business.find_one({"index":f})
            f+=1
            if result:
                for s in range(0, result["business_quantity"]):
                    self.base = result
                    print(self.base["business_id"])
                self.mongoInsert()
            else:
                break
    def mongoInsert(self):
        fg = self.base["business_price"]
        # 不比对福币中没有的business_id
        rt = self.fuyu.count_documents({"business_id": self.base["business_id"]})
        if rt == 0:
            self.rt.write(str(self.base["business_id"])+"\t"+str(fg)+"\n")
            return
        results = self.fuyu.find({"business_id": self.base["business_id"]})
        fel_list = []
        for result in results:
            del self.base["index"]
            if fg == 0:
                break
            if result["get_integral"] <= fg:
                fg = fg -result["get_integral"]
                del result["business_channel"]
                self.base.update(result)
                # qy = self.fuyu.delete_one({"index":result["index"]})
                fel_list.append(result["index"])
                del self.base["_id"]
                qy = self.spbu.insert_one(self.base)
            elif result["get_integral"]>fg:
                if fel_list :
                    qy = self.fuyu.delete_many({"index":{"$in":fel_list}})
                qw = result["get_integral"] - fg
                result["get_integral"] = fg
                qy = self.fuyu.update_one({"index":result["index"]},{'$set':{"get_integral":qw}})

                del result["business_channel"]
                self.base.update(result)
                del self.base["_id"]
                qy = self.spbu.insert_one(self.base)
                fg = 0
        if fg != 0:
            print("请查证",self.base["business_id"])
            self.log.write(str(self.base["business_id"])+"\t"+str(fg)+"\n")
        if fel_list:
            qy = self.fuyu.delete_many({"index": {"$in": fel_list}})
class accountCompare():
    def __init__(self):
        self.compare = pd.DataFrame()
    def original_povit(self):
        # 透视原始表单
        sql = "SELECT * FROM result"
        original = pd.read_sql(sql,engine)
        self.o_p = original.pivot_table(index=["program_id"],values=["get_integral"],aggfunc=np.sum)
        self.o_p.reset_index(inplace=True)
        self.o_p.rename(columns={"get_integral":"start_integral"},inplace=True)
    def jindong_povit(self):
        # 透视京东表单

        jingdong= pd.DataFrame(list(client.fuyu.spbu.find()))
        self.j_p = jingdong.pivot_table(index=["program_id"],values=["get_integral"],aggfunc=np.sum)
        self.j_p.reset_index(inplace=True)
        self.j_p.rename(columns={"get_integral":"fuyu_integral"},inplace=True)

    def run(self):
        self.original_povit()
        self.jindong_povit()
        self.result = pd.merge(self.o_p,self.j_p,how="left",on="program_id")
        self.result["chaizhi"] = self.result["fuyu_integral"] - self.result["start_integral"]
        self.result.to_excel("result\\chaizhi.xlsx")
        if len(self.result[ self.result["chaizhi"]!=0]):
            print("请核对执行过程")
        else:
            print("program_id对比未发现异常")


if __name__ == "__main__":
    dataChange().fuyuQuery()
    dataChange().data_query()
    accountCompare().run()




