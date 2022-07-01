from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String,BigInteger,Float,Text,ForeignKey,Table,MetaData,table
from sqlalchemy.orm import sessionmaker,Session
from sqlalchemy.orm import relationship

metadata = MetaData()
# 创建连接引擎
# echo=True表示会用logger的方式打印传到数据库的SQL
engine = create_engine('mysql+pymysql://root:@localhost/testt?charset=utf8mb4', echo=True)

# 表格对象基类
Base = declarative_base()


class Views(Base):
    __tablename__ = 'jingdong'
    __table__ = Table(__tablename__, Base.metadata, autoload=True, autoload_with=engine)
    __mapper_args__ = {'primary_key': [__table__.c.MyColumnInTable]}

    # id = Column(Integer, primary_key=True)
    # name = Column(String(100))
    # ports = Column(String(100))
    index = Column("index",Integer,)
    business_type = Column("business_type",String)




def dobule_to_dict(self):
    result = {}
    for key in self.__mapper__.c.keys():
        if getattr(self, key) is not None:
            result[key] = str(getattr(self, key))
        else:
            result[key] = getattr(self, key)
    return result


def to_json(all_vendors):
  v = [ ven.dobule_to_dict() for ven in all_vendors ]
  return v

if  __name__ == "__main__":
    # jindong = Table('jingdong',metadata,child_number = Column("child_number",String),autoload=True,autoload_with = engine)
    # guanxi = Table('guanxi', metadata,child_number = Column("child_number",String),autoload=True,autoload_with = engine)
    session = Session(engine)
    # res = session.query(jindong).order_by("order_time")
    # ress = session.execute(jindong.outerjoin(guanxi,guanxi.child_number==jindong.child_number),{"businsess_type":"商品"})
    # # print(res.__getattr__)
    # print([dict(zip(res.keys(),res)) for res in ress])
    res = Session.query(Views)
