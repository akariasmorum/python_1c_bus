import traceback

#получение ID
def getID():
    idgen = misbus.import_script('egisz_sync.idgen')
    unpacked_id = idgen.generate_id(unpacked=True)
    id_x = idgen.pack_id(unpacked_id)
    return id_x
    
#сопоставление данных
def fetchData(data, database, massive, server, user):
    try:
        i=0
        for answer in data:
            ID = answer.select('*/ID').text
            name = answer.select('*/Name').text
            #ищем с1ид в базе
            с1id = lookForC1ID(database, ID)
            #если есть такой
            if len(с1id.data)>0:
                #то сравниваем с1ид с тем, что в базе
                #если названия разные
                    if str(с1id.data[0][6]).replace(' ', '') != str(name).replace(' ', ''):
                        #то добавить в массив
                        massive.append([с1id.data[0][0], с1id.data[0][5], name, server, user, 1 ])
                        #script_output[str(i)] = "{0} vs {1}".format(с1id.data[0][6]),name))
                        #i=i+1
            #если такого с1ид нет в базе            
            else:
                #получаем ID доабвляем сразу в массив, [id ]
                id_x = getID()
                massive.append([ id_x, ID, name, server, user, 0 ])
    except Exception as ex:
        script_output['message'] = traceback.format_exc()    
    
# поиск с1id в базе. если нет, то len(res.data) = 0
def lookForC1ID(database, c1id):
    res = database.execute_query(''' 
        select * from _s_1c_Nomenklature where C1ID=@c1id
    ''', c1id = c1id )
    return res
    
    
def getDataMassive():
    try:
        #массив, куда накапаливаем строки
        massive = []
        
        #подключение к 1С
        remote = misbus.get_external_remote(7)
        operation = remote.get_operation("GetRefBook")
        #Формирование пакета
        namebook = script_input['bookname']
        op_input = operation.build_input_envelope({
            'name': "GetRefBook",
            'children': [
                {'name': "RefBookName", 'text': namebook},
            ],
        })
        op_result = operation.execute_and_parse(op_input)
        
        #парсинг XML
        data = op_result.select_all('//SimpleAnswer')
        database = misbus.get_internal_remote('test-sql')
        
        
        #сервер и юзер
        script = misbus.get_script(28)
        result = script.call({})
        user = result['user']
        serv = result['server']
        
        
        #сопоставление данных из XML с данными из БД, наполнение массива различиями
        fetchData(data, database, massive, serv, user)
        
        return massive
        
    except Exception as ex:
        script_output['message'] = traceback.format_exc()  
        
  
        


    
