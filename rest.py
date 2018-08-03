from System import DateTime

class Ostatki():
    db=None
    dbTable = []
    xmlData=[]
    tableName='_s_1c_ostatki'
    tableNameD='_s_1c_ostatki_d'
    idgen = misbus.import_script('egisz_sync.idgen')
    user=None
    server=None
    def __init__(self,dbAbonent, xmlAbonent, date, user):
        
        self.db = misbus.get_internal_remote(dbAbonent)
        self.user = self.db.execute_query('select id from _s_user where kod={0}'.format(user)).data[0][0]
        self.server = self.db.execute_query('select top 1 ID_Server1 from _a_Option ').data[0][0]
        
        remote = misbus.get_external_remote(xmlAbonent)
        operation = remote.get_operation("GetBalancesOnDate")
        
        #Формирование пакета
        Table = self.db.execute_query("select * from _s_1c_ostatki").data
        for row in Table:
            self.dbTable.append({
                'ID':row[0],
                'ID1':row[1],
                'ID_SERVER':row[2],
                'ID_LOGIN':row[3],
                'DATETIME_C':row[4],
                'IDRESPONSIBLE':row[5],
                'IDPART':row[6],
                'IDBALANCE':row[7],
                'PRISE':row[8],
                'KOL':row[9],
                'SUMMAO':row[10],
                'UNIT':row[11],
                'DATEDOC':row[12],
                'KOLICH':row[13],
                'SUMMAKOL':row[14],
                'PRISEKOL':row[15],
                'PRISEZAKUP':row[16],
                'NAME':row[17],
            })
        op_input = operation.build_input_envelope({
                'name': "GetBalancesOnDate",
                'children': 
                    [
                      {'name': "Date", 'text': date},
                    ],
                })
        op_result = operation.execute_and_parse(op_input)
        
        #парсинг XML
        
        xmldata = op_result.select_all('//Balance')
        
        for answer in xmldata:
            if float(answer.select('*/Count').text)!=0:
                self.xmlData.append(
                    {
                     'IDRESPONSIBLE':answer.select('*/IDResponsible').text, 
                     'IDPART':answer.select('*/IDPart').text,
                     'PRISEKOL':answer.select('*/Prise').text,
                     'IDBALANCE':answer.select('*/IDBalance').text,
                     'KOLICH':answer.select('*/Count').text,
                     'SUMMAKOL':answer.select('*/Sum').text,
                     'UNIT':answer.select('*/Unit').text
                    })
                    
        script_output['num'] = len(self.xmlData)
    def getID(self):
        unpacked_id = self.idgen.generate_id(unpacked=True)
        id_x = self.idgen.pack_id(unpacked_id)
        return str(id_x)            
                
    def fetchData(self):
        intersectionMassive =list(self.dbTable)
        inserted=0
        deleted=0
        insertedD=0
        for answer in self.xmlData:
            found = False
            for i,row in enumerate(self.dbTable):
                coincidence=0
                #если совпадают полностью, то ничего, иначе вставляем это в db
                if str(row['IDRESPONSIBLE']).replace(' ', '')==str(answer['IDRESPONSIBLE']).replace(' ', ''):
                    #наращиеваем чсло совпадений
                    coincidence+=1
                if str(row['IDPART']).replace(' ', '')==str(answer['IDPART']).replace(' ', ''):
                    #наращиеваем чсло совпадений
                    coincidence+=1
                if float(row['PRISEKOL'])==float(answer['PRISEKOL']):    
                    #наращиеваем чсло совпадений
                    coincidence+=1            
                if str(row['IDBALANCE']).replace(' ', '')==str(answer['IDBALANCE']).replace(' ', ''):
                    #наращиеваем чсло совпадений
                    coincidence+=1    
                if float(row['KOLICH'])==float(answer['KOLICH']):
                    #наращиеваем чсло совпадений
                    coincidence+=1
                
                if float(row['SUMMAKOL'])==float(answer['SUMMAKOL']):    
                    #наращиеваем чсло совпадений
                    coincidence+=1        
                if str(row['UNIT']).replace(' ', '')==str(answer['UNIT']).replace(' ', ''):
                    #наращиеваем чсло совпадений
                    coincidence+=1            
                    
                #всего 7 значений в XML    
                
                if coincidence ==7:
                    intersectionMassive[i]=0
                    found = True
                    break
                
            if found==False:
                idx=self.getID() 
                insertRow = dict(answer)
                insertRow['ID'] = idx
                insertRow['ID1'] = idx
                insertRow['ID_SERVER'] = self.server
                insertRow['ID_LOGIN'] =  self.user
                insertRow['DATETIME_C'] = DateTime.Now
                insertRow['DATEDOC'] = script_input['date']
                inserted+=self.insertDataInTable(self.tableName, insertRow)
        
        for row in intersectionMassive:
            if row!=0:
                newRowD = dict(row)
                newRowD['ID_D'] = str(self.getID())
                newRowD['ID_SERVER_D'] = self.server
                newRowD['ID_LOGIN_D'] = self.user
                newRowD['DATETIME_D']= DateTime.Now 
                newRowD['TYPE_D'] = 0
                insertedD+=self.insertDataInTable(self.tableNameD, newRowD)
                deleted+=self.deleteDataFromTable(self.tableName, row['ID'] )
                    
        script_output['inserted'] = inserted
        script_output['deleted'] = deleted
        script_output['insertedD']= insertedD
        
    def subtituteDataInTable(self, table, dictionary, condition):
        query = 'update {0} set '.format(table) + ', '.join(str(k) + '=@ ' + str(k) for k in dictionary) + ' where UNIT='+ condition
        num = db.execute_nonquery(query)
        return num
        
    def insertDataInTable(self,table, dictionary):
        query = 'insert into {0} '.format(table) + '(' + ','.join(str(k) for k in dictionary) + ') values (' + ','.join('@' + str(k) for k in dictionary)+')'
        num = self.db.execute_nonquery(query, **dictionary)
        return num  
    
    def deleteDataFromTable(self, table, idx):
        query = 'delete from {0} where id=@ID'.format(table)
        num = self.db.execute_nonquery(query, ID=str(idx))
        return num        
date = script_input['date']                
x = Ostatki(2, 7, date, 1000)                
x.fetchData()        
        