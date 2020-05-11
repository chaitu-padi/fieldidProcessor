import re
import collections
from pprint import pprint
from collections import defaultdict
from collections import namedtuple
def main():
    tableprop = {}
    fieldprop = defaultdict(list)
    variableprop = {}

    FieldSet = {'winnow': defaultdict(list), 'na': defaultdict(list), 'mfu': defaultdict(list), 'nb_mfu': defaultdict(list), 'formula': defaultdict(list)}
    FullFieldSet = {}

    vfile = open("C:\\Users\\Chaitu-Padi\\PycharmProjects\\pyFieldIdProcessor\\VProperties.txt", "r")
    if vfile.mode == "r":
        vpropContent = vfile.readlines()
        for i in vpropContent:
            variableprop[i.split("=")[0].strip()] = i.split("=")[1].strip()
    #print(variableprop.get("source_country_code"))
    print(variableprop)
    vfile.close()

    tfile = open("C:\\Users\\Chaitu-Padi\\PycharmProjects\\pyFieldIdProcessor\\TProperties.txt", "r")
    if tfile.mode == "r":
        tableContent = tfile.readlines()
        for i in tableContent:
            tableprop[i.split("=")[0].strip()] = i.split("=")[1].strip()
    print(tableprop)
    vfile.close()

    ''' 
    fmapfile = open("C:\\Users\\Chaitu-Padi\\PycharmProjects\\pyFieldIdProcessor\\FieldMapProperties.txt", "r")
    if fmapfile.mode == "r":
        fmapContent = fmapfile.readlines()
        for i in fmapContent:
            fieldprop[i.split(",")[1].strip()].append(i.split(",")[0].strip())
    print (fieldprop)
    fmapfile.close()
    '''
    OIfile = open("C:\\Users\\Chaitu-Padi\\PycharmProjects\\pyFieldIdProcessor\\OIProperties.txt", "r")
    if OIfile.mode == "r":
        OIContent = OIfile.readlines()
        for i in OIContent:
            a = i.split("\t")
            #print(a[2].strip().lower())
            if a[2].strip().lower() == 'winnow':
                FieldSet['winnow'][a[1].strip()].append(a[3].strip())
                FieldSet['winnow'][a[1].strip()].append(a[0].strip())
                FullFieldSet[a[1].strip()] = a[3].strip()
            elif a[2].strip().lower() == "not applicable":
                FieldSet['na'][a[1].strip()].append(a[3].strip())
                FieldSet['na'][a[1].strip()].append(a[0].strip())
                FullFieldSet[a[1].strip()] = a[3].strip()
            elif a[2].strip().lower() == "mfu":
                FieldSet['mfu'][a[1].strip()].append(a[3].strip())
                FieldSet['mfu'][a[1].strip()].append(a[0].strip())
                FullFieldSet[a[1].strip()] = (a[3].strip())
            elif a[2].strip().lower() == "formula field":
                FieldSet['formula'][a[1].strip()].append(a[3].strip())
                FieldSet['formula'][a[1].strip()].append(a[0].strip())
                FullFieldSet[a[1].strip()] = (a[3].strip())
    OIfile.close()
    #for a, b in FieldSet['winnow'].items():
    #    print(a, b[0])


    def createNA():
        fieldlist = '\',\''.join(FieldSet['na'].keys())
        na_where = '\'' + fieldlist + '\''
        print(na_where)
        na = open("C:\\Users\\Chaitu-Padi\\PycharmProjects\\pyFieldIdProcessor\\NATemplate.txt", "r")
        natemplate = na.readlines()
        finalout = []

        for i in natemplate:
            finaltemp = i.replace('@NA_WHERE', na_where)\
                .replace('@OI', tableprop.get('OI').strip())\
                .replace('@ALLPRDMFU', tableprop.get('ALLPRDMFU').strip())\
                .replace('@product', variableprop.get('product').strip())\
                .replace('@allprdods', variableprop.get('allprdods').strip())
            finalout.append(finaltemp)
        print(finalout)
        nawriter = open("C:\\Users\\Chaitu-Padi\\PycharmProjects\\pyFieldIdProcessor\\output\\NA.hql","w")
        nawriter.writelines(finalout)
        nawriter.close()
        na.close()
    createNA()
    '''
    def createWinnow():
        winnowFields = FieldSet['winnow'].keys()
        winnow = open("C:\\Users\\Chaitu-Padi\\PycharmProjects\\pyFieldIdProcessor\\WINNOWTemplate.txt", "r")
        winnowtemplate = winnow.readlines()
        finalout = []

        for i in winnowFields:
            logic = FieldSet['winnow'][i].__getitem__(0)
            logiccode = re.split(" where ", logic, flags=re.IGNORECASE)[0]
            logicwhere = re.split(" where ", logic, flags=re.IGNORECASE)[1]
            print(" logic caliculation : "+logiccode + " and whre is :" +logicwhere)
            for j in winnowtemplate:
                finaltemp = j.replace('@WINNOW_WHERE', '\''+i+'\'')\
                    .replace('@OI', tableprop.get('OI').strip())\
                    .replace('@WINNOW', tableprop.get('WINNOW').strip()) \
                    .replace('@ALLPRDMFU', tableprop.get('ALLPRDMFU').strip()) \
                    .replace('@process_date', variableprop.get('process_date').strip())\
                    .replace('@data_src', variableprop.get('data_src').strip())\
                    .replace('@source_country_code', variableprop.get('source_country_cd').strip())\
                    .replace('@prd_segmt_cd', variableprop.get('prd_segmt_cd').strip())\
                    .replace('@product', variableprop.get('product').strip())\
                    .replace('@allprdods', variableprop.get('allprdods')) \
                    .replace('@LOGICCAL', logiccode) \
                    .replace('@LOGICWHERE', logicwhere)
                finalout.append(finaltemp)
            if i != winnowFields[-1]:
                finalout.append("\n\nunion all\n\n")
        print(finalout)
        winnowwriter = open("C:\\Users\\Chaitu-Padi\\PycharmProjects\\pyFieldIdProcessor\\output\\WINNOW.hql", "w")
        winnowwriter.writelines(finalout)
        winnowwriter.close()
        winnow.close()

    #createWinnow()
    '''



    def createWinnow1():
        winnowFields = FieldSet['winnow'].keys()
        winnow = open("C:\\Users\\Chaitu-Padi\\PycharmProjects\\pyFieldIdProcessor\\WINNOWTemplate.txt", "r")
        winnowtemplate = winnow.read()
        finalout = []

        for i in winnowFields:
            logic = FieldSet['winnow'][i].__getitem__(0)
            logicarraylength = len(re.split(" where ", logic, flags=re.IGNORECASE))
            logiccode = ''
            logicwhere = ''
            if logicarraylength == 2:
                logiccode = re.split(" where ", logic, flags=re.IGNORECASE)[0]
                logicwhere = ' and ' + re.split(" where ", logic, flags=re.IGNORECASE)[1]
            else:
                logiccode = re.split(" where ", logic, flags=re.IGNORECASE)[0]

            print(" logic caliculation : " + logiccode + " and whre is :" + logicwhere)
            finaltemp = winnowtemplate.replace('@WINNOW_WHERE', '\'' + i + '\'') \
                .replace('@OI', tableprop.get('OI').strip()) \
                .replace('@WINNOW', tableprop.get('WINNOW').strip()) \
                .replace('@ALLPRDMFU', tableprop.get('ALLPRDMFU').strip()) \
                .replace('@process_date', variableprop.get('process_date').strip()) \
                .replace('@data_src', variableprop.get('data_src').strip()) \
                .replace('@source_country_code', variableprop.get('source_country_cd').strip()) \
                .replace('@prd_segmt_cd', variableprop.get('prd_segmt_cd').strip()) \
                .replace('@product', variableprop.get('product').strip()) \
                .replace('@allprdods', variableprop.get('allprdods')) \
                .replace('@LOGICCAL', logiccode) \
                .replace('@LOGICWHERE', logicwhere)
            finalout.append(finaltemp)
            if i != winnowFields[-1]:
                finalout.append("\n\nunion all\n\n")
            print(finalout)
            winnowwriter = open("C:\\Users\\Chaitu-Padi\\PycharmProjects\\pyFieldIdProcessor\\output\\WINNOW.hql", "w")
            winnowwriter.writelines(finalout)
            winnowwriter.close()
        winnow.close()

    createWinnow1()

    def createFormula():
        formulaFields = FieldSet['formula'].keys()
        #print(FullFieldSet.viewitems())
        winnowFieldsForFormula = []
        formula = open("C:\\Users\\Chaitu-Padi\\PycharmProjects\\pyFieldIdProcessor\\FORMULATemplate.txt", "r")
        formulatemplate = formula.read()
        finalout = []
        for ff in formulaFields:
            logicwhere = []
            logiccode = []
            min = FieldSet['formula'].get(ff)[0].strip().split(":")[0].replace('SUM(', '')
            max = FieldSet['formula'].get(ff)[0].strip().split(":")[1].replace(')', '')
            fieldmin = str(min)[-4:]
            fieldmax = str(max)[-4:]
            fieldprefix = str(min)[:-4]
            print(fieldprefix, fieldmax, fieldmin)
            dependentFields = []
            for i in range(int(fieldmin), int(fieldmax)+1):
                dependentFields.append(fieldprefix+str(i).zfill(4))
            print(dependentFields)
            for df in dependentFields:
                logic = FullFieldSet.get(df)
                print(logic, df)
                logicarraylength = len(re.split(" where ", logic, flags=re.IGNORECASE))
                if logic.lower().strip() == 'should be blank' or logic.lower().strip() == '':
                    print("its blank for formula dependent fieldid: " + df + " and logic: " + logic)
                elif logicarraylength == 2:
                    print('==2'+"coming here")
                    logiccode.append(re.split(" where ", logic, flags=re.IGNORECASE)[0].lower().strip())
                    wheretemp = re.split(" where ", logic, flags=re.IGNORECASE)[1].lower().strip()
                    wherelist = re.split(" and ", wheretemp, flags=re.IGNORECASE)
                    logicwhere.extend(wherelist)
                else:
                    logiccode.append(re.split(" where ", logic, flags=re.IGNORECASE)[0])

            logiccode = ''.join(set(logiccode))
            logicwhere = ' and '.join(set(logicwhere))
            print("====logiccode====="+logiccode)
            if len(logiccode) != 0:
                finaltemp = formulatemplate.replace('@WINNOW_WHERE', '\'' + ff + '\'') \
                    .replace('@OI', tableprop.get('OI').strip()) \
                    .replace('@WINNOW', tableprop.get('WINNOW').strip()) \
                    .replace('@ALLPRDMFU', tableprop.get('ALLPRDMFU').strip()) \
                    .replace('@process_date', variableprop.get('process_date').strip()) \
                    .replace('@data_src', variableprop.get('data_src').strip()) \
                    .replace('@source_country_code', variableprop.get('source_country_cd').strip()) \
                    .replace('@prd_segmt_cd', variableprop.get('prd_segmt_cd').strip()) \
                    .replace('@product', variableprop.get('product').strip()) \
                    .replace('@allprdods', variableprop.get('allprdods')) \
                    .replace('@LOGICCAL', logiccode) \
                    .replace('@LOGICWHERE', logicwhere)\
                    .replace('@rundate', variableprop.get('rundate'))\
                    .replace('@snap_dt', variableprop.get('snap_dt'))\
                    .replace('@currency', variableprop.get('currency'))
                finalout.append(finaltemp)
            else:
                print(logiccode)
                nat = open("C:\\Users\\Chaitu-Padi\\PycharmProjects\\pyFieldIdProcessor\\NATemplate.txt", "r")
                nattemplate = nat.read()
                finaltemp = nattemplate.replace('@NA_WHERE', '\'' + ff + '\'') \
                    .replace('@OI', tableprop.get('OI').strip()) \
                    .replace('@ALLPRDMFU', tableprop.get('ALLPRDMFU').strip()) \
                    .replace('@product', variableprop.get('product').strip()) \
                    .replace('@allprdods', variableprop.get('allprdods').strip())
                finalout.append(finaltemp)
            if ff != formulaFields[-1]:
                finalout.append("\n\nunion all\n\n")
            print(finalout)
        formulawriter = open("C:\\Users\\Chaitu-Padi\\PycharmProjects\\pyFieldIdProcessor\\output\\FORMULA.hql", "w")
        formulawriter.writelines(finalout)
        formulawriter.close()
        formula.close()

    createFormula()


if __name__=="__main__":
    main()
