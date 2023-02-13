from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_date,regexp_replace,concat_ws,length,split, lit, collect_list
from motordecalidad.constants import *
import operator
from pyspark.sql.types import StructType,StructField

def validateRequisites(object:DataFrame, field:list):
    error_list = list(set(field) - set(object.columns))
    rowsNumber = object.count()
    if len(error_list) == Zero and rowsNumber != Zero :
        return
    elif len(error_list) != Zero:
        raise Exception(f"Falta columna o la columna tiene un nombre distinto. Por favor chequear que el input tiene un esquema válido: {','.join(error_list)}")
    elif rowsNumber == Zero :
        raise Exception("El dataframe de entrada no contiene registros")

#Function that valides the amount of Null registers for certain columns of the dataframe
def validateNull(object:DataFrame,field: str,registersAmount: int,entity: str,threshold):
    dataRequirement = f"El atributo {entity}.{field} debe ser obligatorio (NOT NULL)."
    errorDf = object.filter(col(field).isNull())
    nullCount = object.select(field).filter(col(field).isNull()).count()
    notNullCount = registersAmount - nullCount
    ratio = (notNullCount/ registersAmount) * OneHundred
    return [registersAmount,Rules.NullRule.code,Rules.NullRule.name,Rules.NullRule.property,Rules.NullRule.code + "/" + entity + "/" + field,threshold,dataRequirement,field,ratio,nullCount], errorDf

#Function that valides the amount of Duplicated registers for certain columns of the dataframe
def validateDuplicates(object:DataFrame,fields:List,registersAmount: int,entity: str,threshold: int):
    fieldString = ','.join(fields)
    dataRequirement = f"Todos los identificadores {entity}.({fieldString}) deben ser distintos (PRIMARY KEY)."
    duplicates = object.groupBy(fields).count().filter(col("count") != One)
    errorDf = object.join(duplicates.select(fields), fields, 'inner')
    nonUniqueRegistersAmount = errorDf.count()
    uniqueRegistersAmount = registersAmount - nonUniqueRegistersAmount
    ratio = (uniqueRegistersAmount / registersAmount) * OneHundred

    return [registersAmount,Rules.DuplicatedRule.code,Rules.DuplicatedRule.name,Rules.DuplicatedRule.property,Rules.DuplicatedRule.code + "/" + entity + "/" + fieldString,threshold,dataRequirement,fieldString,ratio,nonUniqueRegistersAmount], errorDf

#Function that valides the equity between certain columns of two objects
def validateReferentialIntegrity(
    testDataFrame: DataFrame,
    referenceDataFrame: DataFrame,
    testColumn: List,
    referenceColumn: List,
    registersAmount: int,
    entity: str,
    referenceEntity: str,
    threshold):
    fieldString = ','.join(testColumn)
    referenceFieldString = ','.join(referenceColumn)
    dataRequirement = f"El atributo {entity}.({fieldString}) debe ser referencia a la tabla y atributo {referenceEntity}.({referenceFieldString}) (FOREIGN KEY)."
    errorDf = testDataFrame.join(referenceDataFrame.select(referenceColumn).toDF(*testColumn), on = testColumn, how = LeftAntiType)
    errorCount = errorDf.count()
    ratio = (One - errorCount/registersAmount) * OneHundred
    return [registersAmount,Rules.IntegrityRule.code,Rules.IntegrityRule.name,Rules.IntegrityRule.property,Rules.IntegrityRule.code + "/" + entity + "/" + fieldString,threshold,dataRequirement,fieldString,ratio, errorCount], errorDf


def validateFormatDate(object:DataFrame,
    formatDate:str,
    columnName:str,
    entity:str,  
    threshold,spark):
    notNullDf = object.filter(col(columnName).isNotNull())
    registerAmount = notNullDf.count()
    dataRequirement = f"El atributo {entity}.{columnName} debe tener el formato {formatDate}."
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    errorDf = notNullDf.withColumn("output", to_date(col(columnName).cast('string'), formatDate))\
    .filter(col("output").isNull()).drop("output")
    errorCount = errorDf.count()
    try:
        ratio = (One - errorCount/registerAmount) * OneHundred
    except:
        ratio = 100.0
    return [registerAmount,Rules.FormatDate.code,Rules.FormatDate.name + " - " + formatDate,Rules.FormatDate.property,Rules.FormatDate.code + "/" + entity + "/" + columnName,threshold,dataRequirement,columnName,ratio, errorCount], errorDf

def validateRange(object:DataFrame,
    columnName:str,
    registerAmount:int,
    entity:str,
    threshold:int,
    minRange = None,
    maxRange = None,
    includeLimitRight:bool = True,
    includeLimitLeft:bool = True,
    inclusive:bool = True,):
    dataRequirement =  f"El atributo {entity}.{columnName}, debe estar entre los valores {minRange} y {maxRange}"
    opel,opeg=chooseComparisonOparator(includeLimitLeft,includeLimitRight,inclusive)

    if inclusive:
        if minRange is None and maxRange is not None:
            errorDf = object.filter(opeg(col(columnName),maxRange))
        elif minRange is not None and maxRange is None:
            errorDf = object.filter(opel(col(columnName), minRange))
        else: 
            errorDf = object.filter(opel(col(columnName),minRange) | opeg(col(columnName),maxRange))       
    else:
        errorDf = object.filter(opel(col(columnName),minRange) & opeg(col(columnName),maxRange))

    errorCount = errorDf.count()
    ratio = (One - errorCount/registerAmount) * OneHundred

    return [registerAmount,Rules.RangeRule.code,Rules.RangeRule.name,Rules.RangeRule.property,Rules.RangeRule.code + "/" + entity + "/" + columnName,threshold,dataRequirement, columnName, ratio, errorCount], errorDf

def chooseComparisonOparator(includeLimitLeft:bool,includeLimitRight:bool,inclusive:bool):
    res=[]
    if inclusive:
        if includeLimitLeft:
            res.append(operator.lt)
        else:
            res.append(operator.le)

        if includeLimitRight:
            res.append(operator.gt)
        else:
            res.append(operator.ge)

    else:
        if includeLimitLeft:
            res.append(operator.ge)
        else:
            res.append(operator.gt)

        if includeLimitRight:
            res.append(operator.le)
        else:
            res.append(operator.lt)
    
    return res[Zero],res[One]


def validateCatalog(object:DataFrame,
    columnName:str, 
    listValues:list,
    registerAmount:int,
    entity:str,
    threshold: int):
    fieldsString = ','.join(listValues)
    dataRequirement = f"El atributo {entity}.{columnName}, debe tomar solo los valores {fieldsString}."
    errorDf = object.filter(~col(columnName).isin(listValues))

    errorCount = errorDf.count()
    ratio = (One - errorCount/registerAmount) * OneHundred

    return [registerAmount,Rules.CatalogRule.code,Rules.CatalogRule.name,Rules.CatalogRule.property,Rules.CatalogRule.code + "/" + entity + "/" + columnName ,threshold,dataRequirement,columnName, ratio, errorCount], errorDf 


def validateForbiddenCharacters(object:DataFrame,
    columnName:str, 
    listValues:list,
    registerAmount:int,
    entity:str,
    threshold: int):

    fieldsString = ','.join(listValues)

    dataRequirement = f"El atributo {entity}.{columnName}, no debe contener los siguentes caracteres: {fieldsString}."

    vals="["+"".join(listValues)+"]"
    object = object.withColumn("replaced", regexp_replace(col(columnName),vals, ""))

    errorDf=object.filter(col(columnName)!=col('replaced')).drop('replaced')

    errorCount = errorDf.count()
    ratio = (One - errorCount/registerAmount) * OneHundred

    return [registerAmount, Rules.ForbiddenRule.code,Rules.ForbiddenRule.name,Rules.ForbiddenRule.property,Rules.ForbiddenRule.code + "/" + entity + "/" + columnName ,threshold,dataRequirement, columnName, ratio, errorCount], errorDf 


def validateType(object:DataFrame,
    data_Type:str,
    columnName:str,
    registerAmount:int,
    entity:str,
    threshold: int):

    dataRequirement = f"El atributo {entity}.{columnName} debe ser de tipo {data_Type}."


    errorDf = object.filter(col(columnName).isNotNull()).withColumn("output", col(columnName).cast(data_Type))\
    .filter(col("output").isNull()).drop("output")

    errorCount = errorDf.count()
    ratio = (One - errorCount/registerAmount) * OneHundred
    return [registerAmount, Rules.Type.code, Rules.Type.name + " - " + data_Type, Rules.Type.property, Rules.Type.code + "/" + entity + "/" + columnName,threshold,dataRequirement,columnName,ratio, errorCount], errorDf

def validateComposision(object: DataFrame,
    columnName:str,
    partialColumns:list,
    registerAmount:int,
    entity: str,
    threshold: int):

    fieldsString = ','.join(partialColumns)
    dataRequirement = f"El atributo {entity}.{columnName} en todas las tablas tiene que tener la siguiente estructura {fieldsString}"
    errorDf = object.filter(col(columnName) != concat_ws("_",*partialColumns))
    errorCount = errorDf.count()
    ratio = (One - errorCount/registerAmount) * OneHundred

    return [registerAmount, Rules.Composision.code, Rules.Composision.name, Rules.Composision.property, Rules.Composision.code + "/" + entity + "/" + columnName,threshold,dataRequirement,columnName,ratio, errorCount], errorDf

def validateLength(object:DataFrame,
    columnName:str,
    registerAmount:int,
    entity,
    threshold,
    minRange = None,
    maxRange = None,):

    dataRequirement =  f"El atributo {entity}.{columnName}, debe contener este numero de caracteres {minRange} y {maxRange}"

    opel,opeg = chooseComparisonOparator(True, True, True)

    if minRange is None and maxRange is not None:
        errorDf = object.filter(opeg(length(col(columnName)), maxRange))
    elif minRange is not None and maxRange is None:
        errorDf = object.filter(opel(length(col(columnName)), minRange))
    else: 
        errorDf = object.filter(opel(length(col(columnName)), minRange) | opeg(length(col(columnName)), maxRange))       

    errorCount = errorDf.count()
    ratio = (One - errorCount/registerAmount) * OneHundred

    return [registerAmount,Rules.LengthRule.code,Rules.LengthRule.name,Rules.LengthRule.property,Rules.LengthRule.code + "/" + entity + "/" + columnName,threshold,dataRequirement, columnName, ratio, errorCount], errorDf


def validateDataType(object:DataFrame,
    columnName:str,
    registerAmount:int,
    entity:str,
    threshold:int,
    data_Type:str):

    dataRequirement =  f"El atributo {entity}.{columnName}, debe ser de tipo {data_Type}"
    try:
        if str(object.schema[columnName].dataType) == data_Type:
            ratio = 0.0
            errorCount = 0
            
        else:
            ratio = 100.0
            errorCount = object.count()
    except:
        try:
            if str(object.schema[columnName.upper()].dataType) == data_Type:
                ratio = 0.0
                errorCount = 0
            
            else:
                ratio = 100.0
                errorCount = object.count()
        except:
            if str(object.schema[columnName.lower()].dataType) == data_Type:
                ratio = 0.0
                errorCount = 0
            
            else:
                ratio = 100.0
                errorCount = object.count()

    return [registerAmount, Rules.DataTypeRule.code,Rules.DataTypeRule.name,Rules.DataTypeRule.property,Rules.DataTypeRule.code + "/" + entity + "/" + columnName,threshold,dataRequirement, columnName, ratio, errorCount]

def validateFormatNumeric(object:DataFrame,
    columnName:str,
    registerAmount:int,
    entity:str,
    threshold:int,
    maxInt:int,
    numDec:int,
    sep:str='.'):

    dataRequirement =  f"El atributo {entity}.{columnName}, debe ser tener el siguiente formato numerico {maxInt} {sep} {numDec}"

    if(str(object.schema[columnName].dataType)!='StringType'):
        object=object.withColumn(columnName,col(columnName).cast('string'))
    object = object.withColumn("int_num",split(object[columnName],sep).getItem(0)).withColumn("dec_num",split(object[columnName],sep).getItem(1))
    errorDf = object.filter(length(col("int_num")) > lit(maxInt)).filter(length(col("dec_num")) > lit(numDec))
    errorCount = errorDf.count()
    ratio = (One - errorCount/registerAmount) * OneHundred

    return [registerAmount, Rules.NumericFormatRule.code,Rules.NumericFormatRule.name,Rules.NumericFormatRule.property,Rules.NumericFormatRule.code + "/" + entity + "/" + columnName,threshold,dataRequirement, columnName, ratio, errorCount], errorDf


def validateOperation(object:DataFrame,
                      columnName:str,
                      registerAmount:int,
                      entity:str,
                      threshold:int,
                      operator:str,
                      input:str,
                      error:float=0):

    cols=object.columns
    res=operation(object,input)
    res.show()

    dataRequirement =  f"El atributo {entity}.{columnName}, no cumple con la ecuacion {columnName}, {operator}, {input}"

    if (operator=='=='):
        err=abs(1-col(columnName)/col(res.columns[-1]))
        errorDf=res.filter(err>error)
    else:
        func=chooseOper(res[res.columns[-1]],operator)
        errorDf=res.filter(func(col(columnName),res[res.columns[-1]]))

    errorCount = errorDf.count()
    ratio = (One - errorCount/registerAmount) * OneHundred

    return [registerAmount, Rules.OperationRule.code,Rules.OperationRule.name,Rules.OperationRule.property,Rules.OperationRule.code + "/" + entity + "/" + columnName,threshold,dataRequirement, columnName, ratio, errorCount], errorDf.select(cols)
 
def operation(object:DataFrame,
                      input:str):
    originalColumns=object.columns
    aux= input.split()
    if(len(aux)==3):
        try:
            num1=float(aux[0])
            oper=chooseOper(lit(num1),aux[1])
            try:
                num2=float(aux[2])
                res=oper(lit(num2))
            except:
                res=oper(object[aux[2]])
        except:
            oper=chooseOper(object[aux[0]],aux[1])
            try:
                num2=float(aux[2])
                res=oper(lit(num2))
            except:
                res=oper(object[aux[2]])
           
        return object.withColumn('ss',res)
    try:
        f=0
        while(True):
           
            par1=aux.index('(')
            par2=aux.index(')')
            newInput=' '.join(aux[par1+1:par2])
            res=operation(object,newInput)
            newInput=' '.join(aux[:par1])+' VAL'+str(f)+' '+' '.join(aux[par2+1:])
            originalColumns.append('VAL'+str(f))
            object=res.withColumnRenamed(res.columns[-1],('VAL'+str(f)))
            object=object.select(originalColumns)
            f+=1
            aux=newInput.split()
           
    except:
        try:
            f=0
            while(True):
                mul1=aux.index('*')
                newInput=' '.join(aux[mul1-1:mul1+2])
                res=operation(object,newInput)
                newInput=' '.join(aux[:mul1-1])+' MUL'+str(f)+' '+' '.join(aux[mul1+2:])
                object=res.withColumnRenamed('ss',('MUL'+str(f)))
                f+=1
                aux=newInput.split()
        except:
            try:
                f=0
                while(True):
                    div1=aux.index('/')
                    newInput=' '.join(aux[div1-1:div1+2])
                    res=operation(object,newInput)
                    newInput=' '.join(aux[:div1-1])+' DIV'+str(f)+' '+' '.join(aux[div1+2:])
                    object=res.withColumnRenamed('ss',('DIV'+str(f)))
                    f+=1
                    aux=newInput.split()
            except:
                try:
                    f=0
                    while(True):
                        res1=aux.index('-')
                        newInput=' '.join(aux[res1-1:res1+2])
                        res=operation(object,newInput)
                        newInput=' '.join(aux[:res1-1])+' RES'+str(f)+' '+' '.join(aux[res1+2:])
                        object=res.withColumnRenamed('ss',('RES'+str(f)))
                        f+=1
                        aux=newInput.split()
                except:
                    try:
                        f=0
                        while(True):
                            su1=aux.index('+')
                           
                            newInput=' '.join(aux[su1-1:su1+2])
                            res=operation(object,newInput)
                            newInput=' '.join(aux[:su1-1])+' SUM'+str(f)+' '+' '.join(aux[su1+2:])
                            object=res.withColumnRenamed('ss',('SUM'+str(f)))
                            f+=1
                            aux=newInput.split()
                    except:
                        return object



def chooseOper(col,op:str):
    if op=='+':
        return col.__add__
    if op=='-':
        return col.__sub__
    if op=='*':
        return col.__mul__
    if op=='/':
        return col.__div__
    if op=='==':
        return operator.ne
    if op=='!=':
        return operator.eq
    if op=='<=':
        return operator.gt
    if op=='>=':
        return operator.lt
    if op=='>':
        return operator.le
    if op=='<':
        return operator.ge

def convert_field_to_struct(object, list_campos: list):
    list_struct_fields = []

    for campo in list_campos:
        type = object.schema[campo].dataType
        list_struct_fields.append(StructField(campo, type))

    return StructType(list_struct_fields)

def measuresCentralTendency(object:DataFrame, columns, spark):
    pivotCol='summary'
    modes=("Mode",)
    columnSchema = [pivotCol]+columns
    
    for i in columns:
        if str(object.schema[i].dataType) == 'BooleanType()':
            object = object.withColumn(i, object[i].cast('string'))
        
        modes=modes+(str(object.groupby(i).count().orderBy("count", ascending=False).first()[0]),)
        
    res=object.select(columns).summary('stddev','mean','min','1%','5%','10%','25%','50%','75%','90%','95%','max')
    modeData = [modes]
    modeDf = spark.createDataFrame(data = modeData,schema = columnSchema)

    res = res.union(modeDf)
    columnsValue = list(map(lambda x: str("'") + str(x) + str("',")  + str(x), columns))
    stackCols = ','.join(x for x in columnsValue)
    df_1 = res.selectExpr(pivotCol, "stack(" + str(len(columns)) + "," + stackCols + ")")\
            .select(pivotCol, "col0", "col1")
    final_df = df_1.groupBy(col("col0")).pivot(pivotCol).agg(concat_ws("", collect_list(col("col1"))))\
                    .withColumnRenamed("col0", pivotCol)
    
    final_df=final_df.withColumnRenamed('summary', 'CAMPOS')\
                        .withColumnRenamed('1%','P1')\
                        .withColumnRenamed('5%','P5')\
                        .withColumnRenamed('25%','P25')\
                        .withColumnRenamed('50%','MEDIANA')\
                        .withColumnRenamed('75%','P75')\
                        .withColumnRenamed('90%','P90')\
                        .withColumnRenamed('95%','P95')\
                        .withColumnRenamed('Mode', 'MODA')\
                        .withColumnRenamed('mean', 'MEDIA')\
                        .withColumnRenamed('stddev','DESVIACION ESTANDAR')\
                        .withColumnRenamed('min','MIN')\
                        .withColumnRenamed('max','MAX')
    
    return final_df