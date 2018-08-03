import traceback
import collections
from System import DateTime

#получение ID
class Integration():
    
    db=None
    dbTable=[]
    xmlData=[]
    table=""
    structure=""
    tableD=""
    structureD=""
    nameIndex=0
    CIDIndex=0
    user=None
    server=None
    idgen = misbus.import_script('egisz_sync.idgen')
    
    dic = { 'Материально ответственные лица': ['_s_1c_Responsible', 6,5],
            'Серии медикаментов':             ['_s_1c_part'       , 6,5],
            'Баланс':                         ['_s_1c_Balans'     , 6,5],
            'Единицы измерения':              ['_s_1c_EdIzm'      , 5,6],
            'Номенклатура':                   ['_s_1c_Nomenklature',6,5],
            'Производитель':                  ['_s_1c_proizv'     , 6,5]
    } 
    def __init__(self, dbAbonent,xmlAbonent, bookName, user):
        self.db = misbus.get_internal_remote(dbAbonent)
        if bookName in self.dic: 
            self.table= self.dic[bookName][0]
            self.nameIndex = self.dic[bookName][1]
            self.CIDIndex = self.dic[bookName][2]
            
            self.tableD = self.table+'_d'
            self.structure = self.getTableStructure(self.table)
            self.structureD = self.getTableStructure(self.tableD)
            script_output['sctr'] =self.structure+ " "+self.structureD    
            remote = misbus.get_external_remote(xmlAbonent)
            operation = remote.get_operation("GetRefBook")
            #Формирование пакета
            
            self.dbTable = self.db.execute_query("select * from {0}".format(self.table)).data
            
            op_input = operation.build_input_envelope({
                    'name': "GetRefBook",
                    'children': 
                        [
                        {'name': "RefBookName", 'text': bookName},
                        ],
                    })
            op_result = operation.execute_and_parse(op_input)
            
            #парсинг XML
            
            data = op_result.select_all('//SimpleAnswer')
            
            for answer in data:
                self.xmlData.append([answer.select('*/ID').text, answer.select('*/Name').text])
                
            self.user = self.db.execute_query('select id from _s_user where kod={0}'.format(user)).data[0][0]
            self.server = self.db.execute_query('select top 1 ID_Server1 from _a_Option ').data[0][0]
            
        else:
            script_output['message'] = 'Такого словаря нет'
    
    def getID(self):
        unpacked_id = self.idgen.generate_id(unpacked=True)
        id_x = self.idgen.pack_id(unpacked_id)
        return id_x
    
    #сопоставление данных
    def fetchData(self):
        
        inserted = 0
        subtituted =0

        respExcludedStructure = self.getTableStructureExc(self.table, ['ID_ORG', 'ID_SOTRUDNIK'])
        edizmExcludedStructure = self.getTableStructureExc(self.table, ['kod1c'])
        #для каждого ответа в xml
        
        for answer in self.xmlData:
            CID = answer[0]
            name = answer[1].replace('×', '*')
            
            c1id = self.lookForItem(self.dbTable, CID)
            
            #если клид найден
            if c1id is not None:
                #сверить названия. если названия отличаются
                if c1id[self.nameIndex].replace(' ', '')!=name.replace(' ', ''):
                    #добавить в массив на замену
                    idx = str(self.getID())
                    if self.table == '_s_1c_Responsible':
                        self.insertDataInTable(self.tableD, self.structureD,[c1id[0], c1id[1], c1id[2], c1id[3], c1id[4], idx, self.server, self.user, DateTime.Now, 1, c1id[5], c1id[6], c1id[7], c1id[8]])
                        num=self.deleteFromTable(self.table, c1id[0])
                        self.insertDataInTable(self.table, self.structure, [c1id[0], c1id[1], self.server, self.user, DateTime.Now, CID, name, c1id[7], c1id[8]] )
                        subtituted+=1
                    else:
                        self.insertDataInTable(self.tableD, self.structureD,[c1id[0], c1id[1], c1id[2], c1id[3], c1id[4], idx, self.server, self.user, DateTime.Now, 1, c1id[5], c1id[6]])
                        num=self.deleteFromTable(self.table, c1id[0])
                        self.insertDataInTable(self.table, self.structure, [c1id[0], c1id[1], self.server, self.user, DateTime.Now, CID, name] )
                        subtituted+=1
                        
            #иначе, если клида нет, то добавить в массив на вставку        
            else:
                id_x = str(self.getID())
                if self.table == '_s_1c_Responsible':
                    self.insertDataInTable(self.table, respExcludedStructure, [id_x, id_x, self.server, self.user,DateTime.Now, CID, name])
                    inserted+=1
                    
                elif self.table=='_s_1c_EdIzm':
                    self.insertDataInTable(self.table, self.structure, [id_x, id_x, self.server, self.user,DateTime.Now, name, CID])
                    
                    inserted+=1
                else:
                    self.insertDataInTable(self.table, self.structure, [id_x, id_x, self.server, self.user,DateTime.Now, CID, name])
                    inserted+=1
                    
                    
        script_output['inserted'] = inserted
        script_output['substituted'] = subtituted
        
      
    
    # поиск с1id в базе. если нет, то len(res.data) = 0
    def lookForItem(self,dbTable, c1id):
        for i,row in enumerate(dbTable):
            if c1id == row[self.CIDIndex].replace(' ', ''):
                return row
        return None        
            
    
    
        
    def insertDataInTable(self,table, structure, values):
        try:
            query = "insert into {0} ({1}) values(".format(table, structure) + ",".join("@"+chr(i+97) for i,val in enumerate(values)) + ")"
            
            dic = {}
            for i, k in enumerate(values):
                dic[chr(i+97)] = k
            #script_output['dic'+str(values[0])] = " / ".join(str(k) for k in values)   
            num = self.db.execute_nonquery(query, **dic)
            return num  
        except Exception as ex:
            script_output[str(values[0])+'_error'] = table+ " " + ' structure:' + structure +" " + " / ".join(str(k) for k in values)
    
    
    def deletingMassive(self):
        
        deleted = 0
        respExcludedStructure = self.getTableStructureExc(self.table, ['ID_ORG', 'ID_SOTRUDNIK'])
        edizmExcludedStructure = self.getTableStructureExc(self.table, ['kod1c'])
        
        for i, val in enumerate(self.dbTable):
            #идем по второму
            found = False
            for k, val2 in enumerate(self.xmlData):
                
                #если находим совпадения, то удаляем из первого массива
                if val[self.CIDIndex].replace(' ', '') == val2[0]:
                    #если нашли такие данные во втором массиве
                    found = True
                    break
                
            if found==False:
                if self.table=='_s_1c_Responsible':
                    self.insertDataInTable(self.tableD, self.structureD, [val[0], val[1], val[2], val[3], val[4], str(self.getID()), self.server, self.user, DateTime.Now, 0, val[5], val[6], val[7], val[8]])
                    
                    num = self.deleteFromTable(self.table, val[0])
                    
                    deleted+=1
                elif self.table=='_s_1c_EdIzm':
                    self.insertDataInTable(self.tableD, self.structureD,[val[0], val[1], val[2], val[3], val[4], str(self.getID()), self.server, self.user, DateTime.Now, 0, val[5], val[6]])
                    
                    num = self.deleteFromTable(self.table, val[0])
                    
                    deleted+=1
                else:
                    self.insertDataInTable(self.tableD, self.structureD, [val[0], val[1], val[2], val[3], val[4], str(self.getID()), self.server, self.user, DateTime.Now, 0, val[5], val[6]])
                    
                    num = self.deleteFromTable(self.table, val[0])
                    
                    deleted+=1
                
        script_output['deleted'] = deleted
     
    
        
#часть 2    


    def getTableStructure(self,table):
        data = self.db.execute_query('''SELECT COLUMN_NAME
                FROM vvk_mias.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = N'{0}' '''.format(table)).data
                
        stri=",".join(str(row[0]) for row in data)
        return stri
    
    def getTableStructureExc(self,table, exclude):
        data = self.db.execute_query('''SELECT COLUMN_NAME
                FROM vvk_mias.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = N'{0}' '''.format(table)).data
        stri=','.join(str(row[0]) for row in data if row[0] not in exclude)
        return stri        

    def selectFromTable(ID):
        query = 'select * from _s_1c_Nomenklature where id = @id'
        varss = self.db.execute_query(query, id=str(ID))
        return varss
    
    def deleteFromTable(self,table, ID):
        query = ''' DELETE FROM {0} WHERE ID=@idx '''.format(table)
        num = self.db.execute_nonquery(query, idx=str(ID))
        return num

books = {
         'Материально ответственные лица',
         'Серии медикаментов', 
         'Баланс', 
         'Единицы измерения',
         'Номенклатура', 
         'Производитель'
        }
        
for book in books:
    integr = Integration(2, 7, book, 1000)
    integr.fetchData()
    integr.deletingMassive()


        
        
