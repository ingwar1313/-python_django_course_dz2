from lxml import html
import json
import databases
import sqlalchemy
from pydantic import BaseModel
from datetime import datetime
import time
import asyncio
import requests
import random
import pandas as pd
######################################
### Формула для подсчета рейтинга ####
######################################
# рейтинг = 3хОЗУ + 2хЧастота - (1/5000)хЦена + 3хДиагональ

coef_ozu = 3
coef_hhz = 2
coef_price = -1/5000
coef_diag = 3

# Заголовки из браузера
headers = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 OPR/93.0.0.0 (Edition Yx 05)"}
# Куда будем складывать. Хорошо бы сделать асинхронку, чтоб он и писал в список, и из него в БД, и удалял из списка записанное в БД, но я не укладываюсь по времени
notebooks_list = []

##############
### Нотик ####
##############

# ссылки для Нотика
notik_urls = ["https://www.notik.ru/search_catalog/filter/work.htm"]
for i in range(2, 3):
     notik_urls.append(f"https://www.notik.ru/search_catalog/filter/work.htm?page={i}")
for url in notik_urls:
    page = requests.get(url=url, headers=headers).text
    tree = html.fromstring(page)
    # a = tree.xpath("//tr[@class='hide-mob']/td/div[2]//a/@href")
    # print(a)
    # print(1/0)
    notebooks_headers = tree.xpath("//tr[@class='hide-mob']")
    notebooks_characteristics = tree.xpath("//tr[@class='goods-list-table']")
    notebooks = list(zip(notebooks_headers, notebooks_characteristics))
    for notebook in notebooks:
        try:
            notebook_dict = {}
            # Дата и время посещения сайта
            notebook_dict["ДАТА_ВРЕМЯ"] = datetime.now()
            # Название, ссылка
            link = notebook[0].xpath('./td/div[2]//a/@href')[0]
            notebook_dict["ССЫЛКА"] = "https://www.notik.ru" + str(link)
            name_part_1 = notebook[0].xpath('./td/div[2]//a/text()')[0]
            name_part_2 = notebook[0].xpath('./td/div[3]//b[@class="wordwrap"]/text()')[0]
            name = (f"{name_part_1} {name_part_2}").strip(" ")
            notebook_dict["НАЗВАНИЕ"] = name
            # Характеристики
            mhz = notebook[1].xpath("./td[2]/text()[3]")[0]
            hhz = int((mhz.split(" МГц"))[0]) / 1000
            notebook_dict["ЧАСТОТА ПРОЦЕССОРА"] = hhz
            ozu = int((notebook[1].xpath("./td[3]/strong[1]/text()")[0]).split(" ГБ")[0].strip(" "))
            notebook_dict["ОБЪЕМ ОЗУ"] = ozu
            ssd = int(notebook[1].xpath("./td[3]/text()[4]")[0].split(" ГБ")[0].strip(" "))
            notebook_dict["ОБЪЕМ SSD"] = ssd
            diag = float(notebook[1].xpath("./td[5]/strong/text()")[0].split('”')[0].strip(" "))
            notebook_dict["ДИАГОНАЛЬ ЭКРАНА"] = diag
            price = int(notebook[1].xpath("./td[8]/a/@ecprice")[0])
            notebook_dict["ЦЕНА"] = price
            notebooks_list.append(notebook_dict)
        except:
            print("Не удалось распознать характеристики у ноутбука")
    print(f"Страница {url} пропарсена, ожидание...")
    time.sleep(random.randint(5,7))

print(f"кол-во ноутов после нотика {len(notebooks_list)}")

#################
### Ситилинк ####
#################
# ситилинк делал первым

# Для тренировки на сохраненной 1й странице
#  url = "https://www.citilink.ru/catalog/noutbuki/?view_type=list"
# req = requests.get(url=url, headers=headers)

# src = req.text
# print(src)

# with open("citilink.html", "w", encoding="utf-8") as file:
#     file.write(src)

# with open("citilink.html", encoding="utf-8") as f:
#     page = f.read()
#  tree = html.fromstring(page)
# ссылки для ситилинка
citilink_urls =  ["https://www.citilink.ru/catalog/noutbuki/?view_type=list"]
for i in range(2, 22):
    citilink_urls.append(f"https://www.citilink.ru/catalog/noutbuki/?view_type=list&_=1673463244067&p={i}")

print(f"кол-во ноутов перед ситилинком {len(notebooks_list)}")
for url in citilink_urls:
    page = requests.get(url=url, headers=headers).text
    tree = html.fromstring(page)
    notebooks = tree.xpath('//div[@class="product_data__gtm-js product_data__pageevents-js ProductCardHorizontal js--ProductCardInListing js--ProductCardInWishlist"]')
    for notebook in notebooks:
        try:
            # Названия характеристик
            characteristics_names_list = list(map(lambda x : x.upper().strip(' :\t\n\r'),notebook.xpath('.//span[@class="ProductCardHorizontal__properties_name"]/text()')))
            # Значения характеристик
            characteristics_values_list = list(map(lambda x : x.upper().strip(' :\t\n\r'),notebook.xpath('.//span[@class="ProductCardHorizontal__properties_value"]/text()')))
            # Объединяем в словарь
            notebook_dict = (dict(zip(characteristics_names_list, characteristics_values_list)))
            # Дата и время посещения сайта
            notebook_dict["ДАТА_ВРЕМЯ"] = datetime.now()
            # Вытаскиваем частоту процессора
            notebook_dict["ЧАСТОТА ПРОЦЕССОРА"] = float(notebook_dict["ПРОЦЕССОР"].split("ГГЦ")[0].strip(" ").split(" ")[-1])
            # Вытаскиваем объем ОЗУ 
            notebook_dict["ОБЪЕМ ОЗУ"] = int(notebook_dict["ОПЕРАТИВНАЯ ПАМЯТЬ"].split("ГБ")[0].strip(" "))
            # Вытаскиваем Объем SSD
            disc_elements = notebook_dict["ДИСК"].split("SSD ")
            # print(disc_elements)
            notebook_dict["ОБЪЕМ SSD"] = int(disc_elements[1].split(" ")[0].strip(" ")) if len(disc_elements) > 1 else 0
            # Вытаскиваем диагональ экрана
            notebook_dict["ДИАГОНАЛЬ ЭКРАНА"] = float(notebook_dict["ЭКРАН"].split('"')[0].strip(" "))
            # Ссылка
            link = "https://www.citilink.ru/product" + notebook.xpath('.//div/a[@class="ProductCardHorizontal__title  Link js--Link Link_type_default"]/@href')[0]
            # print(link)
            notebook_dict["ССЫЛКА"] = link
            # Наименование
            header = json.loads(str(notebook.xpath('.//@data-params')[0]))
            name = header["shortName"]
            # print(name)
            notebook_dict["НАЗВАНИЕ"] = name
            # Цена
            price = header["price"]
            # print(price)
            notebook_dict["ЦЕНА"] = price

            # print(notebook_dict)
            notebooks_list.append(notebook_dict)
        except:
            print("Не удалось распознать характеристики у ноутбука: ", notebook_dict)
    print(f"Страница {url} пропарсена, ожидание...")
    time.sleep(random.randint(5,7))
print(f"кол-во ноутов после ситилинка {len(notebooks_list)}")
#################################
### Создание БД и таблицы #######
#################################

# SQLAlchemy specific code, as with any other app
DATABASE_URL = "sqlite:///./notebooks__.sqlite"
database = databases.Database(DATABASE_URL)
metadata = sqlalchemy.MetaData()


# Таблица по компьютерам
computers = sqlalchemy.Table(
    "computers",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True, autoincrement=True, comment="Идентификатор ноутбука"),
    sqlalchemy.Column("url", sqlalchemy.String, comment="Ссылка на товар"),
    sqlalchemy.Column("visited_at",sqlalchemy.DateTime, comment="Дата-время посещения сайта"),
    sqlalchemy.Column("name", sqlalchemy.String, comment="Наименование товара"),
    sqlalchemy.Column("cpu_hhz", sqlalchemy.Float, comment="Частота процессора, ГГЦ"),
    sqlalchemy.Column("ram_gb", sqlalchemy.Integer, comment="Объем ОЗУ, ГБ"),
    sqlalchemy.Column("ssd_gb", sqlalchemy.Integer, comment="Объем SSD, ГБ"),
    sqlalchemy.Column("price_rub", sqlalchemy.Integer, comment="Цена, руб"),
    sqlalchemy.Column("display_diag", sqlalchemy.Float, comment="Диагональ экрана, дюймов"),
    sqlalchemy.Column("rank", sqlalchemy.Float, comment="Рейтинг")
)

engine = sqlalchemy.create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
    # Уберите параметр check_same_thread когда база не sqlite
)

metadata.create_all(engine)


############################
### Описание записи в БД ###
############################

class ComputerIn(BaseModel):
    id__: int
    url: str
    visited_at = datetime
    name: str
    cpu_hhz: float
    ram_gb:int
    ssd_gb: int
    display_diag: float
    price_rub: int
    rank: float


async def create_computer(computer: ComputerIn):
    query = computers.insert().values(
        url=computer["ССЫЛКА"],
        visited_at=computer["ДАТА_ВРЕМЯ"],
        name=computer["НАЗВАНИЕ"],
        cpu_hhz=computer["ЧАСТОТА ПРОЦЕССОРА"],
        ram_gb=computer["ОБЪЕМ ОЗУ"],
        ssd_gb=computer["ОБЪЕМ SSD"],
        price_rub=computer["ЦЕНА"],
        display_diag=computer["ДИАГОНАЛЬ ЭКРАНА"],
        rank=computer["ОБЪЕМ ОЗУ"]*coef_ozu + computer["ЧАСТОТА ПРОЦЕССОРА"]*coef_hhz + computer["ЦЕНА"]*coef_price + computer["ДИАГОНАЛЬ ЭКРАНА"]*coef_diag
        )
    last_record_id = await database.execute(query)
    return f"id {last_record_id} added to the Database"

###################
### Запись в БД ###
###################


print(f"кол-во ноутов перед записью  {len(notebooks_list)}")
# 408 записей. На ситилинке многие не в наличии, цены нет. Ну сколько смог.
for notebook in notebooks_list:
    try:
        print(asyncio.run(create_computer(notebook)))
    except:
        print(" Не смог залить ", notebook)

############################################
### Запрос на топ 5 по коэффициенту в БД ###
############################################

async def read_top5items():
    query = r"""select id,
    url,
    visited_at as 'Дата-время посещения сайта',
    name as 'Наименование товара',
    cpu_hhz as 'Частота процессора, ГГЦ',
    ram_gb as 'Объем ОЗУ, ГБ',
    ssd_gb as 'Объем SSD, ГБ',
    price_rub as 'Цена',
    display_diag as 'Диагональ экрана, дюймов',
    rank as 'Коэффициент' 
    from 
                (select *
                from computers
                order by rank desc
                limit 5)
                """
    return await database.fetch_all(query)
top = (asyncio.run(read_top5items()))
# Пишем в файл наш топ
pd.DataFrame(top).to_excel("top5notebooks.xlsx")