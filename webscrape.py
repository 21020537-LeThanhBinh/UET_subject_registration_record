import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import insert

for semesterID in range(36, 24, -1):
    URL = "http://112.137.129.87/qldt/index.php?SinhvienLmh%5Bterm_id%5D=0" + str(semesterID) + "&SinhvienLmh_page=" + '1' + "&ajax=sinhvien-lmh-grid&pageSize=10000&r=sinhvienLmh%2Fadmin"
    page = requests.get(URL)
    soup = BeautifulSoup(page.content, "html.parser")

    result = soup.find(id="yw0")

    totalPages = 1
    if result:
        totalPages = len(result.find_all("li", class_="page"))

    object_dict = {'MSV':[], 'Họ_tên':[], 'Ngày_sinh':[], 'Lớp_KH':[], 'Mã_LHP':[], 'Tên_MH':[], 'Nhóm':[], 'Số_TC':[], 'Ghi_chú':[]}

    for pageNum in range(1,totalPages+1):
        URL = "http://112.137.129.87/qldt/index.php?SinhvienLmh%5Bterm_id%5D=0" + str(semesterID) + "&SinhvienLmh_page=" + str(pageNum) + "&ajax=sinhvien-lmh-grid&pageSize=10000&r=sinhvienLmh%2Fadmin"
        page = requests.get(URL)
        soup = BeautifulSoup(page.content, "html.parser")

        objectList = soup.find(id="sinhvien-lmh-grid").find("tbody").find_all("tr")

        for object in objectList:
            row = object.find_all("td")

            msv = row[1].text
            ten = row[2].text
            ngaysinh = row[3].text
            ngaysinh = ngaysinh.split('/')
            ngaysinh = ngaysinh[2] + '-' + ngaysinh[1] + '-' + ngaysinh[0] + ' 12:00:00'
            lopkh = row[4].text
            mlhp = row[5].text
            tenmh = row[6].text
            nhom = row[7].text
            sotc = row[8].text
            gc = row[9].text
            
            object_dict['MSV'].append(msv)
            object_dict['Họ_tên'].append(ten)
            object_dict['Ngày_sinh'].append(ngaysinh)
            object_dict['Lớp_KH'].append(lopkh)
            object_dict['Mã_LHP'].append(mlhp)
            object_dict['Tên_MH'].append(tenmh)
            object_dict['Nhóm'].append(nhom)
            object_dict['Số_TC'].append(sotc)
            object_dict['Ghi_chú'].append(gc)
                
            # print(msv)
            # print(mhp)
            # print(mlhp)
            # print(nhom)
            # print(gc)
            # print()

    df = pd.DataFrame.from_dict(object_dict)
    print(df.head(5))

    def insert_on_duplicate(table, conn, keys, data_iter):
        insert_stmt = insert(table.table).values(list(data_iter))
        on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(insert_stmt.inserted)
        conn.execute(on_duplicate_key_stmt)

    engine = create_engine('mysql+pymysql://root:Binh.191519@localhost/uet_student_data_01')
    # df.to_sql(con=engine, name='dangky_0'+str(semesterID), if_exists='append', index=False)
    df.to_sql(con=engine, name='dangky_0'+str(semesterID), if_exists='append', index=False, method=insert_on_duplicate)

    print("Success")