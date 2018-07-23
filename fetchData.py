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
def fetchData(xmlData, dbTable, insertMassive, substMassive, server, user):
    inserted = 0
    subtituted =0
    db = misbus.get_internal_remote('test-sql')
    #для каждого ответа в xml
    for answer in xmlData:
        CID = answer[0]
        name = answer[1].replace('×', '*')
        c1id = lookForItem(dbTable, CID)
        
        #если клид найден
        if c1id is not None:
            #сверить названия. если названия отличаются
            if c1id[6].replace(' ', '')!=name.replace(' ', ''):
                #добавить в массив на замену
                insertDataInTableD(db,[c1id[0], c1id[1], c1id[2], c1id[3], c1id[4], getID(), server, user, DateTime.Now, 1, c1id[5], c1id[6]])
                num=deleteDataFromTable(db, c1id[0])
                script_output['fetch_delete-'+str(c1id[0])] = str(c1id[0])+" result:" + str(num)
                idn = getID()
                insertDataInTable(db, [idn,idn, server, user, DateTime.Now, CID, name] )
                subtituted+=1
        #иначе, если клида нет, то добавить в массив на вставку        
        else:
            id_x = str(getID())
            insertDataInTable(db,[id_x, id_x, server, user,DateTime.Now, CID, name])
            inserted+=1
    script_output['inserted'] = inserted
    script_output['substituted'] = subtituted
    
      
    
# поиск с1id в базе. если нет, то len(res.data) = 0
def lookForItem(dbTable, c1id):
    for row in dbTable:
        if c1id in row:
            return row
            
            
    
    return None
    
def deletingMassive(delMassive,tableData, indexInTD,  xmlData, indexInXML, server, user):
    db = misbus.get_internal_remote('test-sql')
    deleted = 0
    for i, val in enumerate(tableData):
        #идем по второму
        found = False
        for k, val2 in enumerate(xmlData):
            #если находим совпадения, то удаляем из первого массива
            
            if val[indexInTD] == val2[indexInXML]:
                #если нашли такие данные во втором массиве
                found = True
                break
        
        if found==False:
            insertDataInTableD(db,[val[0], val[1], val[2], val[3], val[4], getID(), server, user, DateTime.Now, 0, val[5], val[6]])
            script_output['deleted_'+str(deleted)] = " _ ".join(str(k) for k in val)
            num = deleteDataFromTable(db, val[0])
            script_output['deleted_'+str(deleted)+'result'] = num
            deleted+=1
            
    script_output['deleted'] = deleted
    
def getDataMassive(db, namebook, insertMassive, subsMassive, delMassive):
    remote = misbus.get_external_remote(7)
    operation = remote.get_operation("GetRefBook")
    #Формирование пакета
    
    dtable = db.execute_query("select * from _s_1c_Nomenklature").data
    
    op_input = operation.build_input_envelope({
            'name': "GetRefBook",
            'children': [
                {'name': "RefBookName", 'text': namebook},
            ],
            })
    op_result = operation.execute_and_parse(op_input)
    
    #парсинг XML
    xml = []
    data = op_result.select_all('//SimpleAnswer')
    
    for answer in data:
        xml.append([answer.select('*/ID').text, answer.select('*/Name').text])
        
    user = db.execute_query('select id from _s_user where kod=1000').data[0][0]
    server = db.execute_query('select top 1 ID_Server1 from _a_Option ').data[0][0]
    
    fetchData(xml, dtable, insertMassive, subsMassive, server, user)        
    deletingMassive(delMassive, dtable,5, xml,0, server, user)  
    
        
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
    query = ''' insert into _s_1c_Nomenklature_d(ID, ID1, ID_SERVER, ID_LOGIN, DATETIME_C,ID_D, ID_SERVER_D, ID_LOGIN_D, DATETIME_D, TYPE_D,  C1ID, NAME)
                    VALUES (@a, @b, @c, @d, @e, @f,  @g, @h, @i, @j, @k, @l )'''
        
    data = database.execute_nonquery(query, 
                a = str(values[0]),
                b = str(values[1]),
                c = values[2],
                d = values[3],
                e = values[4],
                f = str(values[5]),
                g = values[6],
                h=values[7],
                i=values[8],
                j=values[9],
                k=values[10],
                l=values[11])
    return data
    
    
#удалить элемент из таблицы
def deleteDataFromTable(database, ID):
    
    query = ''' DELETE FROM _s_1c_Nomenklature WHERE ID=@idx '''
    num = database.execute_nonquery(query, idx=str(ID))

    return num
    
    

bookname = script_input['bookname']    

db = misbus.get_internal_remote('test-sql')

iM=[]
sM=[]
dM=[]

getDataMassive(db,bookname,iM, sM, dM)


script_output['result'] = "OK"
        
        
  

    


        


    
