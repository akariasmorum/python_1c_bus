def getTableStructure(database, table):
    data = database.execute_query('''SELECT COLUMN_NAME
        FROM vvk_mias.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = N'{0}' '''.format(table))
    str=""
    for i, row in enumerate(data.data):
        if i!=0:
            str+=","
        str+=row[0]    
    return str


def lefTheBattleBegin(massive):
    for row in massive:
        #взять id удаляемого элемента (0ой элемент)
        id = massive[0]
        #взять элемент по этому id из удаляемой таблицы
        
        
def selectFromTable(database, tableName, ID):
    query = 'select * from @table where id = @id'
    vars = database.execute_query(query, table=tableName, id=ID)
    return vars

#select insert delete    
def sid(database, tableName, ID):
    #берем строку по ID
    values = selectFromTable(database, tableName, ID)
    #Берем структуру таблицы
    columnName = getTableStructure(database, tableName)
    #вставляем строку в резервную таблицу _d
    insertDataInTable(database, tableName+'_d', columNames, values)
    #удаляем ту строку из таблицы
    deleteDataFromTable(database, tableName, ID)
    
#вставить это элемент в *_d
def insertDataInTable(database,tableName, columNames, values):
    query = ''' INSERT INTO into N'{0}' ({1}) 
            VALUES ({2}) '''.format(tableName, columNames, values)
    num = database.execute_nonquery(query)  
    return num

#удалить элемент из таблицы
def deleteDataFromTable(database, tableName, ID):
    query = ''' DELETE FROM @table  
            WHERE ID=@id '''
    num = database.execute_nonquery(query, table=tableName, id=ID)  
    return num
    
    
    

