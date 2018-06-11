package cn.ac.iie.base.dpi
case class DpiUrlLog(
                      host:String,
                      uri:String,
                      _interface:String,
                      appTypeCode:String,
                      visitDate:String,
                      visitTime:String,
                      protoType:String,
                      userType:String,//
                      operator:String,
                      province:String,
                      system:String,//
                      ulBytes:String,//
                      dlBytes:String,//
                      httpVersion:String,//
                      userAgent:String,//
                      urlType:String,//
                      contentType:String,//
                      refer:String,//
                      cookie:String,//
                      contentLen:String,//
                      browserType:String,//
                      requestNum:String,//
                      etl_date:String,
                      userProv:String,//
                      userOperator:String,
                      userSystem:String
                    ){

}
//Host,URI,Interface,AppTypeCode,visitDate, AS visitTime,ProtoType,userType, userProv, userOperator, userSystem,operator,province,system,HTTPVersion,UserAgent,URLType,ContentType,Refer,Cookie,ContentLen,(double)STRSPLIT(fivedata,'#').$1 AS ULBytes,(double)STRSPLIT(fivedata,'#').$3 AS DLBytes,(long)STRSPLIT(fivedata,'#').$2 AS requestNum,BrowserType;
