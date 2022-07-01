from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String,BigInteger,Float,Text,ForeignKey,Table,MetaData
from sqlalchemy.orm import sessionmaker,Session
from sqlalchemy.orm import relationship
import datetime
metadata = MetaData()
# 创建连接引擎
# echo=True表示会用logger的方式打印传到数据库的SQL
engine = create_engine('mysql+pymysql://root:@localhost/financial?charset=utf8mb4', echo=False)

# 表格对象基类
Base = declarative_base()

cursor = Session(engine)