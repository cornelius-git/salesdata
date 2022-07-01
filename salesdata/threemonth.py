import pymysql
import pandas as pd
import datetime
conn = pymysql.connect(user = "root",host = "127.0.0.1",port = 3306,charset = "utf8mb4",database = "testt")
cursor = conn.cursor(cursor = pymysql.cursors.DictCursor)


class shujuduihuan:
    def __init__(self):
        self.base_pd = pd.DataFrame()
        self.gh = 6
        self.log = open("record.text",mode="a+",encoding="utf-8")
    def jindong(self):
        print(self.gh)
        index = 0
        while True:
            sql = "SELECT * from jingdong LEFT JOIN guanxi on jingdong.child_number" \
                  " = guanxi.child_number WHERE jingdong.`indexs`={} and" \
                  " jingdong.business_code = guanxi.business_code".format(index)
            cursor.execute(sql)
            self.base = cursor.fetchall()
            if len(self.base) == 1:
                self.base = self.base[0]
                # 一件商品多个订单
                for i in range(0,self.base["bussiness_number"]):
                    self.business_id = self.base["business_id"]
                    print(self.business_id)
                    self.integral = self.base["price"]
                    self.business_code = self.base["business_code"]
                    self.fuyu()
            elif len(self.base)>1:
                # 同一个订单下，在福域上面多次下单
                for j in self.base:
                    self.base = j
                    # 一件商品多个订单
                    for i in range(0, self.base["bussiness_number"]):
                        self.business_id = self.base["business_id"]
                        print(self.business_id)
                        self.integral = self.base["price"]
                        self.business_code = self.base["business_code"]
                        self.fuyu()
            else:
                break
            index += 1
        self.base_pd.to_excel("新的结果2.xlsx")
        self.log.close()
        print(index)

    def fuyu(self):
        fg = self.integral
        sql = "SELECT * FROM fuyu WHERE business_id = '{}' ORDER BY get_create_time asc".format(self.business_id)
        cursor.execute(sql)
        result = cursor.fetchall()
        if not result :
            print("未查询到",self.business_id)
            self.log.write(str(self.business_id)+"\n")
        for s_dict in result:
            integral = s_dict["get_integral"]
            if fg == 0:
                return
            elif integral <= fg:
                self.base.update(s_dict)
                sql = "DELETE FROM fuyu WHERE `index` = {}".format(s_dict["index"])
                cursor.execute(sql)
                conn.commit()
                fg = fg - integral
                self.base_pd = pd.concat([self.base_pd,pd.DataFrame(self.base,index=[1])])
            elif fg < integral:
                s_dict["get_integral"] = fg
                self.base.update(s_dict)
                if integral - fg == 0:
                    sql = "DELETE FROM fuyu WHERE `index` = {}".format(s_dict["index"])
                else:
                    sql = "UPDATE fuyu set get_integral = {} WHERE `index` = {}".format(integral-fg,s_dict["index"])
                cursor.execute(sql)
                conn.commit()
                fg = 0
                self.base_pd = pd.concat([self.base_pd,pd.DataFrame(self.base,index=[1])])
                return



if __name__ == "__main__":
    s = datetime.datetime.now()
    shujuduihuan().jindong()
    print(datetime.datetime.now() - s)



