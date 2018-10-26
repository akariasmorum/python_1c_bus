from System import DBNull
from System import DateTime
import json
import base64
import re


def getTable(iddoc):
    misbus.log("Декодирую ID")

    id = base64.b64decode(iddoc)   

    misbus.log("Подключаюсь к базе")
    db = misbus.get_internal_remote(2)

    misbus.log("Выполняю запрос")
    result = db.execute_query('''
    Select distinct 
	'' AS VERSION
	,'' AS SOFTWARE
	,'' AS VERSION_SOFTWARE
	,rtrim(_s_sotrudnik_user.fam)+' '
		+RTRIM(_s_sotrudnik_user.Name)+' '
		+RTRIM(_s_sotrudnik_user.oth)		AS AUTHOR
	,isnull(_s_sotrudnik_user.phone,'')		AS PHONE
	,isnull(_s_sotrudnik_user.EMAIL,'')		AS EMAIL
	,cASe when		_s_NETRUDTYPE.KOD='09'
				OR  _s_NETRUDTYPE.KOD='12'
				OR  _s_NETRUDTYPE.KOD='13'
				OR  _s_NETRUDTYPE.KOD='14'
				OR  _s_NETRUDTYPE.KOD='15'
				then doc.snilsPredstavit
				else DOC.SNILS	end			AS SNILS
	,RTRIM(_s_PACIENT.fam)					as SURNAME
	,RTRIM(_s_PACIENT.Name)					as NAME
	,+RTRIM(_s_PACIENT.oth)					AS PATRONIMIC

	,'0'									AS BOZ_FLAG
	,''										AS LPU_EMPLOYER
	,''										AS LPU_EMPL_FLAG
	,isnull(DOC.Num,'')						AS LN_CODE
	,isnull(DOC.OLDLIST_NUM,'')				AS PREV_LN_CODE
	,ISNULL(_s_NETRUDTYPE.KOD,'')			AS PRIMARY_FLAG
	,cASe when doc.dubl=1	then 1
							else 0	end		AS DUPLICATE_FLAG
	,LEFT(CONVERT(VARCHAR, convert(Date, doc.DATEDOC), 120), 10)							AS LN_DATE
	,ISNULL(_s_ORG.netrud_name,'')			AS LPU_NAME
	,ISNULL(_s_ORG.netrud_adres,'')			AS LPU_ADDRESS
	,(select top 1 s2.ogrn
			from _s_orgstrukture s1
			left outer join _s_orgstruktureinf s2 on s1.id=s2.id_orgstrukTure
			where s1.id_org=doc.id_org and s1.type_org=1) 
											AS LPU_OGRN
	,cASe when		_s_NETRUDTYPE.KOD='09'
				then LEFT(CONVERT(VARCHAR, convert(Date, doc.DATER), 120), 10)
				else LEFT(CONVERT(VARCHAR, convert(Date, _s_PACIENT.DATER), 120), 10)	end	AS  BIRTHDAY
	,cASe when doc.vidan_sex='М'	
								then 0 
								else 1 end	AS GENDER
	,ISNULL(_s_NETRUDVID.KODfss,'')			AS REASON1	
	,ISNULL(_s_NetrudVidDop.KOD,'')			AS REASON2
	,''										AS REASON3
	,''										AS DIAGNOZ
	,isnull(DOC.Num_osnov,'')				AS PARENT_CODE
	,cASe when _s_NETRUDTYPE.KOD='05' 
				then LEFT(CONVERT(VARCHAR, convert(Date, DOC.DateRod), 120), 10) 
		  when _s_NETRUDTYPE.KOD='08' 
				then LEFT(CONVERT(VARCHAR, convert(Date, DOC.Putevka_Home), 120), 10)
				else '' end					AS DATE1
	,cASe  when _s_NETRUDTYPE.KOD='08' 
				then LEFT(CONVERT(VARCHAR, convert(Date, DOC.PUTEVKA_END), 120), 10)
				else '' end					AS DATE2
	,DOC.Putevka_Num				AS VOUCHER_NO
	,DOC.Putevka_OGRN			AS VOUCHER_OGRN
	/*уход за 1 больным*/
	, left(doc.uxod1_vozrASt,2)				AS SERV1_AGE			/* кол-во лет*/
	, right(doc.uxod1_vozrASt,2)			AS SERV1_MM				/* кол-во месяцев*/
	,ISNULL(_s_Rodstvo1.kod,'')				AS SERV1_RELATION_CODE	/* кол-во лет*/
	,ISNULL( doc.uxod1_fio,'')				AS SERV1_FIO			/* ФИО */
	/*уход за 2 больным*/
	, left(doc.uxod2_vozrASt,2)				AS SERV2_AGE			/* кол-во лет*/
	, right(doc.uxod2_vozrASt,2)			AS SERV2_MM				/* кол-во месяцев*/
	,ISNULL(_s_Rodstvo2.kod,'')				AS SERV2_RELATION_CODE	/* кол-во лет*/
	,ISNULL( doc.uxod2_fio,'')				AS SERV2_FIO			/* ФИО */
	,cASe when doc.srok_do12=0	then ''
		else cast(doc.srok_do12 as char(1))
		END									AS PREGN12W_FLAG
	,(SELECT TOP 1 ISNULL(_s_NetrudNoRegimVid.KOD,'')			/*'КОД НАРУШЕНИЯ РЕЖИМА*/
		FROM _d_Netrud_NoRegimVid
			LEFT OUTER JOIN _s_NetrudNoRegimVid  
				ON _d_Netrud_NoRegimVid.ID_NETRUDNOREGIMVID=_s_NetrudNoRegimVid.ID
		WHERE ID_DOC=VSE.ID_DOC)			AS HOSPITAL_BREACH_CODE

	,ISNULL(cast(_S_invalid_grup.KOD as char(1)),'')
											AS MSE_INVALID_GROUP
	,ISNULL(_s_netrudclose.KOD,'')			AS MSE_RESULT
	,ISNULL(doc.NewList_Num,'')				AS NEXT_LN_CODE
	,'СОСТОЯНИЕ ЭЛН'						AS LN_STATE
	,'ХЭШ ДАННЫХ ЭЛН'						AS LN_HASH
	,LEFT(CONVERT(VARCHAR,convert(Date, vse.DATEHOME), 120), 10) AS TREAT_DT1
	,LEFT(CONVERT(VARCHAR,convert(Date, vse.DATEEND), 120), 10)	 AS TREAT_DT2
	,_s_dol.NAME_NETRUDK					AS TREAT_DOCTOR_ROLE
	,rtrim(_s_sotrudnik.fam)+' '
		+RTRIM(_s_sotrudnik.Name)+' '
		+RTRIM(_s_sotrudnik.oth)			AS TREAT_DOCTOR
	,VSE.ID									AS ID_DOC
	,DOC.ID_DOC								AS ID

	,1										AS ECP_SOTRUDNIK
	,case when vk='1' then 1 
		else 0 end							AS ECP_SOTRUDNIKVK
	,1										AS ECP_ORG
	,vse.vk									as fl_vk
	,isnull(rtrim(vk.fam)+' '
		+RTRIM(vk.Name)+' '
		+RTRIM(vk.oth),'')					AS TREAT_CHAIRMAN
	,'ПРЕД ВК'                         	    as TREAT_CHAIRMAN_ROLE
 from _d_Netrud_Osvob vse 
 left outer join _d_Netrud_Osvob_fss vse_fss on vse.ID_DOC=vse.ID_DOC
 left outer join _d_Netrud_H  doc			on vse.ID_DOC=doc.ID_DOC
 left outer join _s_sotrudnik				on VSE.id_sotrudnik_Osvob=_s_sotrudnik.id 
 left outer join _s_dol						on VSE.id_dol=_s_dol.id 
 left outer join _s_ORG						on doc.ID_ORG=_s_ORG.id 
 left outer join _s_NETRUDREGIM				on doc.ID_NETRUDREGIM =_s_NetrudRegim.id 
 left outer join _s_NETRUDTYPE				on doc.ID_NETRUDTYPE =_s_NETRUDTYPE.id 
 left outer join _s_NETRUDVID				on doc.ID_NETRUDVID	 =_s_NETRUDVID.id 
 left outer join _s_NetrudVidDop			on doc.ID_NetrudVidDop	 =_s_NetrudVidDop.id 
 left outer join _s_Rodstvo _s_Rodstvo1		on doc.ID_Rodstvo1	 =_s_Rodstvo1.id 
 left outer join _s_Rodstvo _s_Rodstvo2		on doc.ID_Rodstvo2	 =_s_Rodstvo2.id 
 left outer join _s_invalid_grup			on doc.ID_invalid	 =_s_invalid_grup.id 
 left outer join _s_PACIENT					on doc.ID_PACIENT	 =_s_PACIENT.id 
 left outer join _s_netrudclose				on doc.ID_netrudclose	 =_s_netrudclose.id 
 left outer join _s_sotrudnik vk			on doc.id_Sotrudnik_vk	 =vk.id 
 left outer join _s_dol vk_dol				on vk.id_dol	 =vk_dol.id  
 /* user */
 left outer join _s_sotrudnik _s_sotrudnik_user					on DOC.ID_SOTRUDNIK_USER=_s_sotrudnik_user.id 
 WHERE VSE.ID_DOC = @iddoc
            ''', iddoc=bytes(id))

    misbus.log("Составляю словарь")
    misbus.log(str(len(result.data)))
    dbTable = []
    for i, row in enumerate(result.data):
        dic = {}
        for column, value in zip(result.columns, result.data[0]):
            
            
            if value != DBNull.Value and value != "":
                
                if (column.db_type == 'varchar' or column.db_type == 'char'):
                    dic[column.name] = value.rstrip()
                else:
                    dic[column.name] = value
        dic['SNILS'] = re.sub('\-*\s*', '', dic['SNILS'])        
        dbTable.append(dic)

    

    return dbTable


def sortData(dbTable):
    rowChildren = {
        'SNILS': None,
        'SURNAME': None,
        'NAME': None,
        'PATRONIMIC': None,
        'BOZ_FLAG': None,
        'LPU_EMPLOYER': None,
        'LN_CODE': None,
        'PREV_LN_CODE': None,
        'PRIMARY_FLAG': None,
        'DUPLICATE_FLAG': None,
        'LN_DATE': None,
        'LPU_NAME': None,
        'LPU_ADDRESS': None,
        'LPU_OGRN': None,
        'BIRTHDAY': None,
        'GENDER': None,
        'REASON1': None,
        'REASON2': None,
        'REASON3': None,
        'DIAGNOS': None,
        'PARENT_CODE': None,
        'VOUCHER_NO': None,
        'VOUCHER_OGRN': None,
        'SERV1_AGE': None,
        'SERV1_MM': None,
        'SERV1_RELATION_CODE': None,
        'SERV1_FIO': None,
        'SERV2_AGE': None,
        'SERV2_MM': None,
        'SERV2_RELATION_CODE': None,
        'SERV2_FIO': None,
        'LN_STATE': None
    }

    jsonb = {}
    jsonb['TREAT_PERIODS'] = []
    
    
    #присваиваем всех первых потомков
    
    for column in dbTable[0]:
        if column in rowChildren:
            jsonb[column] = dbTable[0][column]
            
    #################################
    
    #если есть нарушение режима, то оно будет в каждой строке, поэтому берем только из первой
    if 'HOSPITAL_BREACH_COD' in dbTable[0]:
            jsonb['HOSPITAL_BREACH'] = []
            jsonb['HOSPITAL_BREACH'].append({
                'HOSPITAL_BREACH_COD': dbTable[0]['HOSPITAL_BREACH_COD'],
                'HOSPITAL_BREACH_DT': dbTable[0]['HOSPITAL_BREACH_DT']
            })
    ########################
    
    
    misbus.log("Добавляю даты")

    # для каждой позиции в dbTable
    # все позиции ROW оставить как есть 
    # а все TREAT_PERIODS закинуть в TREAT_PERIODS
    
    
    for i, row in enumerate(dbTable):
        #составляю период
        fullPeriod = {
                'TREAT_PERIOD': {
                'TREAT_DT1': row['TREAT_DT1'],
                'TREAT_DT2': row['TREAT_DT2'],
                'TREAT_DOCTOR_ROLE': row['TREAT_DOCTOR_ROLE'],
                'TREAT_DOCTOR': row['TREAT_DOCTOR'],
            }
        }    
        #добавляю председателя если сть                    
        if 'TREAT_CHAIRMAN' in row:
            fullPeriod['TREAT_CHAIRMAN_ROLE'] = row['TREAT_CHAIRMAN_ROLE']
            fullPeriod['TREAT_CHAIRMAN'] = row['TREAT_CHAIRMAN']
        
        #добавляю полынй период 
        jsonb['TREAT_PERIODS'].append(fullPeriod)
        
    #превращаю словарь в json строку
    j = json.dumps(jsonb, ensure_ascii=False)

    return str(j)

def getJson(id):
    table = getTable(id)
    
    s=sortData(table)
    return s
    
'''    
id = bytes(chr(17) +
           chr(11) +
           chr(1) +
           chr(14) +
           chr(47) +
           chr(22) +
           chr(0) +
           chr(0) +
           chr(0) +
           chr(0) +
           chr(16) +
           chr(0) +
           chr(0) +
           chr(178) +
           chr(30))
misbus.log(str(id))'''
