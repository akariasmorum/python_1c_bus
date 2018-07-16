import traceback

def fetchData(data, database, massive):
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
                    massive.append([с1id.data[0][0], с1id.data[0][5], name ])
                    #script_output[str(i)] = "{0} vs {1}".format(с1id.data[0][6]),name))
                    #i=i+1
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
    
    
def getDataMassive():
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
        
        return massive
        
        #for i, row in enumerate(massive):
        #    script_output[str(i)] = "{0} / {1} / {2}".format(row[0], row[1], row[2])
        
    except Exception as ex:
        script_output['message'] = traceback.format_exc()    
        
try:
    mass = getDataMassive()

class AtomID:
    year = 0
    month = 0
    day = 0
    hour =0
    minutes =0
    seconds =0
    
    uniqueX=0
    unique1=0
    unique2=0
    unique3=0
    
    nServ =0
    
    user1=0
    user2=0
    user3=0
    
    def __init__(self):
        try:
            result = database.execute_query('select convert(varchar(19), getdate(), 120)').data[0][0]
            year = int(datex[2:4])
            month=  int(datex[5:7])
            day= int(datex[8:10])
            hour= int(datex[11:13])
            minutes= int(datex[14:16])
            seconds= int(datex[17:19])
            
            serv = database.execute_query('select номер сервера')
            nServ = serv.data[0][0]
            
            import math
            serv1 = math.modf(nServ/(255*255))
            ost = nServ - (serv1*255*255)
            serv2 = math.modf(ost/255)
            serv3 = ost - (serv*255)
            
            user1=0
            user2=0
            user3 = database.execute_query('select номер юзера').data[0][0]
    except Exception as Ex:
        return str(Ex)
        
    def getID():
        try:    
            if uniqueX<256*256*256:
                b = uniqueX.to_bytes(3, 'big')
                unique1 = b[0]
                unique2= b[1]
                unique3=b[3]
                return  (chr(year) + chr(month)+ chr(day) + chr(hour)+
                chr(minutes) + chr(seconds)+chr(unique1)+ chr(unique2)+chr(unique3) + chr(serv1) +
                chr(serv2) + chr(serv3)+ chr(user1) + chr(user2) + chr(user3))
            else:
        except Exception as Ex:
            return str(Ex)
    
