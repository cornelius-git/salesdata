# 创建会话的类
DBSession = sessionmaker(bind=engine)

# 表格类
class JingDong(Base):
    """jingdong order table"""
    __tablename__ = 'jingdong'  # 表名
    __table_args__ = {'extend_existing': False}
    # 表结构
    index = Column(BigInteger,primary_key=True)
    child_number = Column(Text)
    origin_number = Column(BigInteger)
    order_time = Column(Text)
    business_type = Column(Text)
    business_code = Column(Text)
    business_name = Column(Text)
    business_tax = Column(BigInteger)
    bussiness_number = Column(BigInteger)
    bussiness_tax_price = Column(Float)
    business_total_tax_price = Column(Float)
    business_total_price = Column(Float)
    bussiness_price = Column(Float)
    pay_price = Column(Float)
    bussiness_total_number = Column(BigInteger)
    flow_fee = Column(Float)
    real_pay_price = Column(Float)


class guanxi(Base):
    """jingdong order table"""
    __tablename__ = 'jingdong'  # 表名
    __table_args__ = {'sqlite_autoincrement': True}

    # 表结构
    index = Column(BigInteger)
    child_number = Column(Text)
    business_id =  Column(Text)
    price =  Column(BigInteger)

class fuyu():
    """jingdong order table"""
    __tablename__ = 'jingdong'  # 表名
    __table_args__ = {'sqlite_autoincrement': True}

    # 表结构
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(32), nullable=False)  # 有些数据库允许不指定String的长度
    age = Column(Integer, default=0)
    password = Column(String(64), unique=True)

def init_db():
    engine = get_engine()
    Base.metadata.create_all(engine)
    print('Create table successfully!')

def get_engine():
    return create_engine(
        "mysql+pymysql://root:luckygxf@localhost:3306/test",
        encoding="utf-8",
        echo=True
    )


def get_session():
    db_session = sessionmaker(bind=engine)
    return db_session()

def query_user():
    session = get_session()
    users = session.query(JingDong).all()
    session.close()
    return users
