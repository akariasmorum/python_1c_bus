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
def fetchData(db, table, xmlData, dbTable, server, user, nameIndex):
    
    inserted = 0
    subtituted =0
    
    tableD = table+'_d'
    structure = getTableStructure(db, table)
    structureD = getTableStructure(db, tableD)
    respExcludedStructure = getTableStructureExc(db, table, ['ID_ORG', 'ID_SOTRUDNIK'])
    edizmExcludedStructure = getTableStructureExc(db, table, ['kod1c'])
    #для каждого ответа в xml
    
    for answer in xmlData:
        CID = answer[0]
        name = answer[1].replace('×', '*')
        c1id = lookForItem(dbTable, CID)
        
        #если клид найден
        if c1id is not None:
            #сверить названия. если названия отличаются
            if c1id[nameIndex].replace(' ', '')!=name.replace(' ', ''):
                #добавить в массив на замену
                script_output['inserted_'+str(c1id[0])] = c1id[nameIndex].replace(' ', '') + " / " + name.replace(' ', '')
                if table == '_s_1c_Responsible':
                    insertDataInTable(db, tableD, structureD,[c1id[0], c1id[1], c1id[2], c1id[3], c1id[4], str(getID()), server, user, DateTime.Now, 1, c1id[5], c1id[6], c1id[7], c1id[8]])
                    num=deleteFromTable(db, table, c1id[0])
                    insertDataInTable(db, table, structure, [c1id[0], c1id[1], server, user, DateTime.Now, CID, name, c1id[7], c1id[8]] )
                    subtituted+=1
                else:
                    insertDataInTable(db, tableD, structureD,[c1id[0], c1id[1], c1id[2], c1id[3], c1id[4], str(getID()), server, user, DateTime.Now, 1, c1id[5], c1id[6]])
                    num=deleteFromTable(db, table, c1id[0])
                    insertDataInTable(db, table, structure, [c1id[0], c1id[1], server, user, DateTime.Now, CID, name] )
                    subtituted+=1
                    
                '''elif table=='_s_1c_EdIzm':
                    insertDataInTable(db, tableD, structureD,[c1id[0], c1id[1], c1id[2], c1id[3], c1id[4], str(getID()), server, user, DateTime.Now, 1, c1id[5], c1id[6]])
                    num=deleteFromTable(db, table, c1id[0])
                    insertDataInTable(db, table, structure, [c1id[0], c1id[1], server, user, DateTime.Now, name, CID] )
                    subtituted+=1'''
                
                    
        #иначе, если клида нет, то добавить в массив на вставку        
        else:
            id_x = str(getID())
            if table == '_s_1c_Responsible':
                insertDataInTable(db,table, respExcludedStructure, [id_x, id_x, server, user,DateTime.Now, CID, name])
                inserted+=1
                
            elif table=='_s_1c_EdIzm':
                insertDataInTable(db,table, structure, [id_x, id_x, server, user,DateTime.Now, name, CID])
                
                inserted+=1
                
            else:
                insertDataInTable(db,table, structure, [id_x, id_x, server, user,DateTime.Now, CID, name])
                inserted+=1
    script_output['inserted'] = inserted
    script_output['substituted'] = subtituted
    
      
    
# поиск с1id в базе. если нет, то len(res.data) = 0
def lookForItem(dbTable, c1id):
    for row in dbTable:
        if c1id in row:
            return row
            
            
    
    return None
    
def insertDataInTable(db, table, structure, values):
    query = "insert into {0} ({1}) values(".format(table, structure) + ",".join("@"+chr(i+97) for i,val in enumerate(values)) + ")"
    script_output['query'] = query
    dic = {}
    for i, k in enumerate(values):
        dic[chr(i+97)] = k
    script_output['dic'+str(values[0])] = values[6]   
    num = db.execute_nonquery(query, **dic)
    return num 
    
    
def deletingMassive(db, table, tableData, indexInTD,  xmlData, indexInXML, server, user):
    
    deleted = 0
    tableD=table+'_d'
    structure = getTableStructure(db, table)
    structureD = getTableStructure(db, tableD)
    respExcludedStructure = getTableStructureExc(db, table, ['ID_ORG', 'ID_SOTRUDNIK'])
    edizmExcludedStructure = getTableStructureExc(db, table, ['kod1c'])
    
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
            if table=='_s_1c_Responsible':
                insertDataInTable(db, tableD, structureD,[val[0], val[1], val[2], val[3], val[4], str(getID()), server, user, DateTime.Now, 0, val[5], val[6], val[7], val[8]])
                script_output['deleted_'+str(deleted)] = " _ ".join(str(k) for k in val)
                num = deleteFromTable(db, table, val[0])
                script_output['deleted_'+str(deleted)+'result'] = num
                deleted+=1
            elif table=='_s_1c_EdIzm':
                insertDataInTable(db, tableD, structureD,[val[0], val[1], val[2], val[3], val[4], str(getID()), server, user, DateTime.Now, 0, val[5], val[6], val[7]])
                script_output['deleted_'+str(deleted)] = " _ ".join(str(k) for k in val)
                num = deleteFromTable(db, table, val[0])
                script_output['deleted_'+str(deleted)+'result'] = num
                deleted+=1
            else:
                insertDataInTable(db, tableD, structureD, [val[0], val[1], val[2], val[3], val[4], str(getID()), server, user, DateTime.Now, 0, val[5], val[6]])
                script_output['deleted_'+str(deleted)] = " _ ".join(str(k) for k in val)
                num = deleteFromTable(db, table, val[0])
                script_output['deleted_'+str(deleted)+'result'] = num
                deleted+=1
            
    script_output['deleted'] = deleted
    
def getDataMassive(db, namebook, table, nameIndex, cidIndex):
    remote = misbus.get_external_remote(7)
    operation = remote.get_operation("GetRefBook")
    #Формирование пакета
    
    dtable = db.execute_query("select * from {0}".format(table)).data
    
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
    
    fetchData(db, table, xml, dtable, server, user, nameIndex)        
    deletingMassive(db, table, dtable, cidIndex, xml, 0, server, user) 
    
        
#часть 2    


def getTableStructure(database, table):
    data = database.execute_query('''SELECT COLUMN_NAME
            FROM vvk_mias.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = N'{0}' '''.format(table)).data
            
    stri=",".join(str(row[0]) for row in data)
    return stri
    
def getTableStructureExc(database, table, exclude):
    data = database.execute_query('''SELECT COLUMN_NAME
            FROM vvk_mias.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = N'{0}' '''.format(table)).data
    stri=','.join(str(row[0]) for row in data if row[0] not in exclude)
    return stri        

def selectFromTable(database, ID):
    query = 'select * from _s_1c_Nomenklature where id = @id'
    varss = database.execute_query(query, id=str(ID))
    return varss
    
    
#select insert delete    

        
#вставить это элемент в *_d



    
#удалить элемент из таблицы
def deleteDataFromTable(database, ID):
    
    query = ''' DELETE FROM _s_1c_Nomenklature WHERE ID=@idx '''
    num = database.execute_nonquery(query, idx=str(ID))
    return num
    
def deleteFromTable(db, table, ID):
    query = ''' DELETE FROM {0} WHERE ID=@idx '''.format(table)
    num = db.execute_nonquery(query, idx=str(ID))
    return num
    
dic = { 'Материально ответственные лица': ['_s_1c_Responsible', 6,5],
        'Серии медикаментов':['_s_1c_part', 6,5],
        'Баланс':['_s_1c_Balans', 6,5],
        'Единицы измерения':['_s_1c_EdIzm',5,7],
        'Номенклатура':['_s_1c_Nomenklature', 6, 5],
        'Производитель':['_s_1c_proizv', 6, 5]
}  

bookname = script_input['bookname']    
table = ""
nameIndex =0
CIDIndex=0
if bookname in dic: 
    table= dic[bookname][0]
    nameIndex = dic[bookname][1]
    CIDIndex = dic[bookname][2]
else:
    script_output['нет такого словаря']
    
db = misbus.get_internal_remote('test-sql')
script_output['словарь'] = bookname
script_output['table'] = table

getDataMassive(db,bookname, table, nameIndex, CIDIndex)


script_output['result'] = "OK"
        
        
