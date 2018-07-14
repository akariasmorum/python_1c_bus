import traceback

try:
    #массив, куда накапаливаем строки
    massive = []
    #подключение к 1С
    remote = misbus.get_external_remote(7)
    operation = remote.get_operation("GetRefBook")
    namebook = script_input['bookname']
    #Формирование пакета
    op_input = operation.build_input_envelope({
        'name': "GetRefBook",
        'children': [
            {'name': "RefBookName", 'text': namebook},
        ],
    })
    op_result = operation.execute_and_parse(op_input)
    data = op_result.select_all('//SimpleAnswer')
    database = misbus.get_internal_remote('test-sql')
    
    fetchData(data, database, massive)
    
    for i, row in massive:
        script_output[str(i)] = "{0} / {1} / {3}".format(row[0], row[1], row[2])
        
    
    def fetchData(data, database, massive):
        
        for answer in data:
            ID = answer.select('*/ID').text
            name = answer.select('*/Name').text
            #ищем с1ид в базе
            с1id = lookForC1ID(database, c1id)
            #если есть такой
            if len(c1id.data)>0:
                #то сравниваем с1ид с тем, что в базе
                #если названия разные
                    if c1id.data[0][6] != name:
                        #то добавить в массив
                        massive.append([c1id.data[0][0], c1id.data[0][5], name ])
            #если такого с1ид нет в базе            
            else:
                #сразу добавлям в массив
                massive.append([ 0, ID, name ])
                
            
        
        
    # поиск с1id в базе. если нет, то len(res.data) = 0
    def lookForC1ID(database, c1id):
        res = database.execute_query(''' 
            select * from _s_1c_Nomenklature where C1ID=@c1id
        ''', c1id = c1id )
        return res
    
    def fetchData()    
    
except Exception as ex:
    script_output['message'] = traceback.format_exc()    
