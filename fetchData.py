import traceback
import collections
from System import DateTime

#получение ID
def getID():
    idgen = misbus.import_script('egisz_sync.idgen')
    unpacked_id = idgen.generate_id(unpacked=True)
    id_x = idgen.pack_id(unpacked_id)
    return id_x
    
#сопоставление данных
def fetchData(data, database, massive, server, user):
    try:
        #сделаю пока десяток
        for answer in data:
            ID = answer.select('*/ID').text
            name = answer.select('*/Name').text.replace('×', '*')
            #ищем с1ид в базе
            с1id = lookForC1ID(database, ID)
            #если есть такой
            if len(с1id.data)>0:
                #то сравниваем с1ид с тем, что в базе
                #если названия разные
                    if str(с1id.data[0][6]).replace(' ', '') != str(name).replace(' ', ''):
                        #то добавить в массив
                        massive.append([с1id.data[0][0],с1id.data[0][0], server,  user, DateTime.Now, с1id.data[0][5], name, 1])
                        
            #если такого с1ид нет в базе            
            else:
                #получаем ID доабвляем сразу в массив, [id ]
                id_x = getID()
                massive.append([ id_x,id_x, server, user,  DateTime.Now, ID, name, 0])
                
    except Exception as ex:
        script_output['message'] =  "fetchdata error" + traceback.format_exc()    
    
# поиск с1id в базе. если нет, то len(res.data) = 0
def lookForC1ID(database, c1id):
    res = database.execute_query(''' 
        select * from _s_1c_Nomenklature where C1ID=@c1id
    ''', c1id = c1id )
    return res
    
    
def getDataMassive(database, namebook):
    try:
        
        #массив, куда накапаливаем строки
        massive = []
        
        #подключение к 1С
        remote = misbus.get_external_remote(7)
        operation = remote.get_operation("GetRefBook")
        #Формирование пакета
        
        op_input = operation.build_input_envelope({
            'name': "GetRefBook",
            'children': [
                {'name': "RefBookName", 'text': namebook},
            ],
        })
        op_result = operation.execute_and_parse(op_input)
        
        #парсинг XML
        data = op_result.select_all('//SimpleAnswer')
        #database = misbus.get_internal_remote('test-sql')
        
        
        #сервер и юзер
        script = misbus.get_script(28)
        result = script.call({})
        user = result['user']
        serv = result['server']
        
        
        #сопоставление данных из XML с данными из БД, наполнение массива различиями
        fetchData(data, database, massive, serv, user)
        
        return massive
        
    except Exception as ex:
        script_output['message'] = "get data massive error" + traceback.format_exc()  
        
  
#часть 2    


def getTableStructure(database, table):
    try:
        data = database.execute_query('''SELECT COLUMN_NAME
            FROM vvk_mias.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = N'{0}' '''.format(table))
        stri=""
        for i, row in enumerate(data.data):
            if i!=4: #убираем datetime
                if i!=0:
                    stri+=","
                stri+=row[0]    
        script_output['structure'] = stri
        return str
        
    except Exception as ex:
        script_output['message'] = "getTableStructure error" + traceback.format_exc()  


def selectFromTable(database, ID):
    query = 'select * from _s_1c_Nomenklature where id = @id'
    varss = database.execute_query(query, id=str(ID))
    return varss
    
    
#select insert delete    

        
#вставить это элемент в *_d

def insertSelectByID(database, ID):
    
    query = '''insert into {0} ({1})
                select {2}
                from {3} 
                where C1ID='e98f782f-5776-11e5-8a23-0015172adb1d' '''.format(tableNameInto, columns, columNames)
                
#убрать повторяющийся код
def insertDataInTable(database, values):
    query = ''' insert into _s_1c_Nomenklature (ID, ID1, ID_SERVER, ID_LOGIN, DATETIME_C, C1ID, NAME)
                VALUES (@a, @b, @c, @d, @e, @f, @g)'''
    
    data = database.execute_nonquery(query, 
        a = str(values[0]),
        b = str(values[1]),
        c = values[2],
        d = values[3],
        e = values[4],
        f = values[5],
        g = values[6].replace('×', '*'))
    
    return data
    
def insertDataInTableD(database, values):
    query = ''' insert into _s_1c_Nomenklature_d(ID, ID1, ID_SERVER, ID_LOGIN, DATETIME_C,  C1ID, NAME)
                    VALUES (@a, @b, @c, @d, @e, @f,  @g )'''
        
    data = database.execute_nonquery(query, 
                a = str(values[0]),
                b = str(values[1]),
                c = values[2],
                d = values[3],
                e = values[4],
                f = values[5],
                g = values[6])
    return data
    
    
#удалить элемент из таблицы
def deleteDataFromTable(database, ID):
    
    query = ''' DELETE FROM _s_1c_Nomenklature WHERE ID=@id '''
    num = database.execute_nonquery(query, id=str(ID))  
    return num
    

bookname = script_input['bookname']    

db = misbus.get_internal_remote('test-sql')
massive = getDataMassive(db,bookname)

for i, row in enumerate(massive):
    if row[7] == 1:
        
        script_output[str(i)+ "_ascii"] = ','.join(str(ord(c)) for c in row[6])
        values = selectFromTable(db, row[0])
        script_output[str(i)+ '_selected'] = " / ".join(str(k) for k in values.data[0])
        
        ins = insertDataInTableD(db,values.data[0])
        script_output[str(i)+ 'inserted_in_D'] = ins
        #удаляем ту строку из таблицы
        num = deleteDataFromTable(db,  row[0])
        script_output[str(i)+ '_deleted'] = num
        
    num = insertDataInTable(db, row)
    script_output[str(i)+ '_insertedResult'] = num   
        
    script_output[str(i)] = " / ".join(str(k) for k in row)
