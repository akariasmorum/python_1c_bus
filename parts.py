from System import DateTime



class Parts():
    db=None
    dbTable = []
    xmlData=[]
    tableName='_s_1c_partAll'
    tableNameD='_s_1c_partAll_d'
    idgen = misbus.import_script('egisz_sync.idgen')
    user=None
    server=None
    
    
    def __init__(self,dbAbonent, xmlAbonent, user):
        
        self.db = misbus.get_internal_remote(dbAbonent)
        self.user = self.db.execute_query('select id from _s_user where kod={0}'.format(user)).data[0][0]
        self.server = self.db.execute_query('select top 1 ID_Server1 from _a_Option ').data[0][0]
        
        remote = misbus.get_external_remote(xmlAbonent)
        operation = remote.get_operation("GetAllPartsData")
        
        #Формирование пакета
        Table = self.db.execute_query("select * from {0}".format(self.tableName)).data
        for row in Table:
            self.dbTable.append({
                'ID':row[0],
                'ID1':row[1],
                'ID_SERVER':row[2],
                'ID_LOGIN':row[3],
                'DATETIME_C':row[4],
                'C1ID':row[5],
                'IDNOMENCLATURE':row[6],
                'RELEASEDATE':row[7],
                'EXPIRATIONDATE':row[8],
                'EXTERNALNAME':row[9],
                'IDPROIZV':row[10],
                'IDCOUNTRY':row[11]    
            })
        op_input = operation.build_input_envelope({
                'name': "GetAllPartsData"
                })
        op_result = operation.execute_and_parse(op_input)
        
        #парсинг XML
        
        xmldata = op_result.select_all('//PartData')
        lowerDate = DateTime.ParseExact("01/01/1753", 'dd/mm/yyyy', None)
        highDate = DateTime.ParseExact("12/31/9999", 'mm/dd/yyyy', None)
        
        for answer in xmldata:
            dictionary={}
            
            s=answer.select('*/ReleaseDate').text
            releaseDate=None
            if s!=None:
                try:
                    releaseDate = DateTime.Parse(s)
                    
                except Exception:
                    releaseDate= None
            if releaseDate!=None:
                dictionary['RELEASEDATE'] = releaseDate
                
            expirationDate=None    
            s= answer.select('*/ExpirationDate').text   
            if s!=None:
                try:
                    expirationDate = DateTime.Parse(s)
                except Exception:
                    expirationDate= None
            if expirationDate>lowerDate and expirationDate<highDate:
                dictionary['EXPIRATIONDATE'] = expirationDate
                
            nomenclature = answer.select('*/IDNomenclature').text    
            if nomenclature!=None:
                dictionary['IDNOMENCLATURE'] = nomenclature
                
                
            external = answer.select('*/ExternalName').text   
            if external!=None:
                dictionary['EXTERNALNAME'] = external
                
            proizv = answer.select('*/IDManufacturer').text     
            if proizv!=None:
                dictionary['IDPROIZV'] = proizv
                
            country = answer.select('*/IDCountry').text     
            if country!=None:
                dictionary['IDCOUNTRY'] = country
                
                
            C1ID = answer.select('*/ID').text     
            if C1ID!=None:
                dictionary['C1ID'] = C1ID
                
            self.xmlData.append(dictionary)
                
        
        '''
        dictionary={}
        s=xmldata[42].select('*/ReleaseDate').text
        releaseDate=None
        if s!=None:
            DateTime.TryParse(s, releaseDate)
        if releaseDate!=None:
            dictionary['RELEASEDATE'] = releaseDate
            
        expirationDate=None    
        s= xmldata[42].select('*/ExpirationDate').text   
        if s!=None:
            DateTime.TryParse(s,expirationDate)
        if expirationDate!=None:
            dictionary['EXPIRATIONDATE'] = releaseDate  
            
            
        nomenclature = xmldata[42].select('*/IDNomenclature').text    
        if nomenclature!=None:
            dictionary['IDNOMENCLATURE'] = nomenclature
            
            
        external = xmldata[42].select('*/ExternalName').text   
        if external!=None:
            dictionary['EXTERNALNAME'] = external
            
        proizv = xmldata[42].select('*/IDManufacturer').text     
        if proizv!=None:
            dictionary['IDPROIZV'] = proizv
            
        country = xmldata[42].select('*/IDCountry').text     
        if country!=None:
            dictionary['IDCOUNTRY'] = country
        
        C1ID = xmldata[42].select('*/ID').text     
        if C1ID!=None:
            dictionary['C1ID'] = C1ID
            
        for x in range(40, 55):    
            date = None
            s = xmldata[x].select('*/ExpirationDate').text
            date=None
            if s!=None
                try:
                    date = DateTime.Parse()
                except Exception:
                    date=None
                    
            script_output[str(x)] = str(date)
                                
        '''
            
        
        
    def getID(self):
        unpacked_id = self.idgen.generate_id(unpacked=True)
        id_x = self.idgen.pack_id(unpacked_id)
        return str(id_x)            
                
    def fetchData(self):
        intersectionMassive =list(self.dbTable)
        inserted=0
        deleted=0
        insertedD=0
        for a, answer in enumerate(self.xmlData):
            found = False
            coincidence=0
            
            for i,row in enumerate(self.dbTable):
                
                #если совпадают полностью, то ничего, иначе вставляем это в db
                for key in answer:
                    if str(answer[key]).replace(' ', '') == str(row[key]).replace(' ', ''):
                        
                        coincidence+=1
                    
                if coincidence == len(answer):
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
        script_output['all'] = len(self.xmlData)
      
    def removekey(self,d, key):
        r = dict(d)
        del r[key]
        return r
        
    def subtituteDataInTable(self, table, dictionary, condition):
        query = 'update {0} set '.format(table) + ', '.join(str(k) + '=@ ' + str(k) for k in dictionary) + ' where UNIT='+ condition
        num = db.execute_nonquery(query)
        return num
        
    def insertDataInTable(self,table, dictionary):
        try:
            query = 'insert into {0} '.format(table) + '(' + ','.join(str(k) for k in dictionary) + ') values (' + ','.join('@' + str(k) for k in dictionary)+')'
            num = self.db.execute_nonquery(query, **dictionary)
            #script_output[dictionary['ID']] = " / ".join(str(key) + ':' + str(dictionary[key]) for key in dictionary)
            #script_output[str(dictionary['ID']) + '_query'] = query
            return num
        except Exception as ex:
            script_output[str(dictionary['ID'])] = " / ".join(str(key) + ':' + str(dictionary[key]) for key in dictionary)
            script_output[str(dictionary['ID'])+'error'] = str(ex)
            return 0
        
    def deleteDataFromTable(self, table, idx):
        query = 'delete from {0} where id=@ID'.format(table)
        num = self.db.execute_nonquery(query, ID=str(idx))
        return num        
        
x = Parts(2, 7, 1000)                
x.fetchData()        
        