import datetime
import os

import pandas as pd
from sqlalchemy import Column, Date, DECIMAL, ForeignKey, Integer, String, Table
from sqlalchemy import create_engine, MetaData
from sqlalchemy import select

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class DbConnection:
    def __init__(self):
        self.engine = create_engine(f"sqlite:///{os.path.join(BASE_DIR, 'db')}")
        self.meta = MetaData(self.engine)


class Tables(DbConnection):
    def __init__(self):
        super().__init__()

    def create_tables(self):
        proxies = Table(
            'proxies', self.meta,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('host', String(128)),
            Column('status', String(128))
        )

        nft = Table(
            'nft', self.meta,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('url', String(128))
        )

        statistic = Table(
            'statistic', self.meta,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('proxy', ForeignKey("proxies.id")),
            Column('nft', ForeignKey("nft.id")),
            Column("date_view", Date),
            Column("views", Integer),
            Column("koii_rating", DECIMAL)
        )

        days_statistic = Table(
            'days_statistic', self.meta,
            Column('nft', ForeignKey("nft.id")),
            Column("date", Date),
            Column("views", Integer),
            Column("koii_rating", DECIMAL)
        )

        self.meta.create_all(self.engine)

    def filling_tables(self):
        proxies_df = pd.read_csv("..\\data\\proxy.csv")
        proxies_df['status'] = 'active'
        nft_df = pd.read_csv("..\\data\\links.csv")
        nft_df = nft_df.loc[:, 'url']
        proxies_df.to_sql('proxies', self.engine, index=None, if_exists='append')
        nft_df.to_sql('nft', self.engine, index=None, if_exists='append')


tables = Tables()
tables.create_tables()


# tables.filling_tables()


class Service(DbConnection):
    def __init__(self):
        super().__init__()
        self.table_proxies = Table('proxies', self.meta, autoload=True)
        self.table_nft = Table('nft', self.meta, autoload=True)
        self.table_statistic = Table('statistic', self.meta, autoload=True)
        self.table_days_statistic = Table('days_statistic', self.meta, autoload=True)

    def get_proxies(self):
        with self.engine.connect() as conn:
            proxies = conn.execute(
                self.table_proxies.select().where(self.table_proxies.c.status == 'active')
            ).fetchall()
        return proxies

    def get_nft(self):
        with self.engine.connect() as conn:
            nft = conn.execute(
                self.table_nft.select()
            ).fetchall()
        return nft

    def get_proxy_to_nft(self, nft_id, count_days, count_proxy_to_one_nft):
        with self.engine.connect() as conn:
            busy_proxies = conn.execute(
                select(self.table_statistic.c.proxy).where(
                    self.table_statistic.c.nft == nft_id,
                    self.table_statistic.c.date_view > (
                            datetime.datetime.utcnow() - datetime.timedelta(days=count_days)).date()
                )
            ).fetchall()
            # print(busy_proxies)
            busy_proxies = [busy_proxy[0] for busy_proxy in busy_proxies]
            busy_proxies_ = conn.execute(
                select(self.table_statistic.c.proxy).where(
                    self.table_statistic.c.date_view == datetime.datetime.utcnow().date()
                )
            ).fetchall()
            # print(busy_proxies_)
            busy_proxies_ = [busy_proxy[0] for busy_proxy in busy_proxies_]

            busy_proxies = list(set(busy_proxies + busy_proxies_))
            # print(busy_proxies)
            free_proxies = conn.execute(
                self.table_proxies.select().where(~self.table_proxies.c.id.in_(busy_proxies),
                                                  self.table_proxies.c.status == 'active'),
                # self.table_proxies.select().where(self.table_proxies.c.id==28)
            ).fetchmany(count_proxy_to_one_nft)
        return free_proxies

    def write_statistic(self, nft_id, proxy_id, views, koii_rating):
        with self.engine.connect() as conn:
            conn.execute(
                self.table_statistic.insert().values(
                    nft=nft_id,
                    proxy=proxy_id,
                    date_view=datetime.datetime.utcnow(),
                    views=views,
                    koii_rating=koii_rating
                )
            )

    def set_failed_status_to_proxy(self, proxy):
        proxy_id = proxy.id
        with self.engine.connect() as conn:
            conn.execute(
                self.table_proxies.update().where(self.table_proxies.c.id == proxy_id).values(status='failed')
            )

    def feeling_days_statistic(self, nft_id, views, koii_rating):
        with self.engine.connect() as conn:
            conn.execute(
                self.table_days_statistic.insert().values(
                    nft=nft_id,
                    date=datetime.datetime.utcnow(),
                    views=views,
                    koii_rating=koii_rating
                )
            )

    def get_random_proxies(self, count_nft):
        with self.engine.connect() as conn:
            proxies = conn.execute(
                self.table_proxies.select()
            ).fetchmany(count_nft)
            return proxies
