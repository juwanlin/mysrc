#include "CommissionMgr.h"
#include <iostream>
#include <vector>
#include <iostream>
#include <stdio.h>
using namespace std;

namespace BM35 {

void CommissionMgr::handleConfig(const std::string& name, const std::string& value)
{
    loadConfiguration(value);
}

void CommissionMgr::argumentDefine(const std::string& name, const std::string& value)
{
}

void CommissionMgr::initialize(Bmco::Util::Application& self)
{
    ServerProcessTools::initialize(self);
}


void CommissionMgr::uninitialize()
{
    INFORMATION_LOG("CommissionMgrMgr is now shutdown .....");
    ServerProcessTools::uninitialize();
}

void CommissionMgr::displayArgument(void)
{
}

int CommissionMgr::main(const std::vector<std::string>& args)
{

    displayArgument();

    if (!init()) {
        cout << "No effective commission rule... " << endl;
        return 0;
    }

    cout << "Begin commission calculation... " << endl;

    for (int i = 0; i < m_vecRule.size(); i++)
    {
        //计算
        calcCommission(m_vecRule[i]);
    }

    cout << "End commission calculation... " << endl;

    return 0;
}

void CommissionMgr::calcCommission(CommissionRule rule)
{
    DEFINE_QUERY_EX(qry,"bill");
    char sSql[1024]={0};
    int iCount = 0;
    CommissionInst inst;
    
    //例 出账日期：2021.11.1 ， 实例 5.1-5.31 生效 ， 10.1 满5， 11.1 结算
    Date d1, d2, d3;
    char sCycleBeginDate[15] = {0};
    char sEffDate[15] = {0};
    char sTempYM[5] = {0};
    
    d3.setDay(1);
    d3.getTimeString(sCycleBeginDate, "yyyymmdd");
    d3.getTimeString(sTempYM, "yyyymm");
    
    sprintf(sSql,
      "select b.obj_id, a.offer_inst_id, b.eff_date from offer_inst a, offer_obj_inst_rel b \
        where a.offer_inst_id=b.offer_inst_id and a.offer_id=%ld \
        and b.exp_date>=STR_TO_DATE('%s','%%Y%%m%%d')", rule.m_lOfferID, sCycleBeginDate);

    qry->Prepare(sSql) ;
    qry->Open();

    while (qry->Next()) {
        std::string sTmpID = qry->Field(0).AsString();
        inst.m_lProdInstID = atol(sTmpID.c_str());
        inst.m_lOfferInstID = qry->Field(1).AsLong();
        strcpy(sEffDate,qry->Field(2).AsString());
        inst.m_iSettTimes = 1;
        inst.m_iSettYM = atoi(sTempYM);
        
        d2.parse(sEffDate);
        int iDiffValue = d1.diffMon(d2);
        if (iDiffValue % rule.m_iInterval == 0) {
	        //满足类型条件
	        if (check(rule, inst)) {
	            m_vecInst.push_back(inst);
	            iCount++;
	        }
	        //入库
	        if (iCount%1000 == 0)
	        	resultInDB(rule);
        }

    }
    qry->Close();
    
	resultInDB(m_vecRule[i]);

    cout<< "RuleID=> " << rule.m_iRuleID << "  Result record=> "<< iCount << endl;
    
}

void CommissionMgr::resultInDB(CommissionRule rule)
{
    if (m_vecInst.empty()) {
    	return;
    }

    DEFINE_QUERY_EX(qry,"bill");
    //DEFINE_QUERY_EX(qry,"bill");
    char sSql[1024]={0};

    sprintf(sSql,
      "insert into commission_inst(rule_id,offer_inst_id,prod_inst_id,commission,sett_times,sett_ym,create_date) \
      values(:rule_id, :offer_inst_id, :prod_inst_id, :commission, :sett_times, :sett_ym, now())");

    qry->Prepare(sSql);
    try{
        for (int i = 0; i < m_vecInst.size(); i++)
        {
            qry->SetParameter("rule_id", rule.m_iRuleID);
            qry->SetParameter("commission", rule.m_iCommission/100);
            qry->SetParameter("offer_inst_id", m_vecInst[i].m_lOfferInstID);
            qry->SetParameter("prod_inst_id", m_vecInst[i].m_lProdInstID);
            qry->SetParameter("sett_times", m_vecInst[i].m_iSettTimes);            
            qry->SetParameter("sett_ym", m_vecInst[i].m_iSettYM);   

            qry->Execute();

            if(i%100 == 0) {
                qry->Commit();
            }
        }
        qry->Commit();
        qry->Close();
    }catch(CDBException &e) {
        ERROR_LOG("数据库错误，信息如下：\nSQL->%s\nERROR->%s", e.GetErrSql(), e.GetErrMsg());
        qry->Close();
    }catch(...) {
        ERROR_LOG("insert into commission_inst fail");
        qry->Close();
    }

    m_vecInst.clear();
}

bool CommissionMgr::check(CommissionRule rule, CommissionInst &inst)
{
	//检查次数	
	ProdInstIndex::iterator itr = mapProdInstIndex.find(inst.m_lProdInstID);
	if (itr != mapProdInstIndex.end()) {
		//本月已结算
		if (itr->second.m_iSettYM == inst.m_iSettYM)
			return false;
		
		//同一实例，结算次数超出
		if (itr->second.first == inst.m_lOfferInstID && itr->second.second >= rule.m_iTotalTimes) {
			return false;
		} else {
			inst.m_iSettTimes = itr->second.m_iSettTimes+1;
		}
	}	

    //0-新号上台; 1-携号上台(后付费); 999-不限
    if (rule.m_iProdInstType==999) {
        return true;
    }
    
    DEFINE_QUERY_EX(qry,"bill");
    //DEFINE_QUERY_EX(qry,"userinfo");
    char sSql[1024]={0};
    int iValue = 0;

    //属性ID：60019    上台类型：0新号上台，1携号上台(后付费)
    sprintf(sSql,
      "select ifnull(attr_value,'0') from prod_inst_attr where prod_inst_id=%ld and attr_id=60019 and exp_date>now()", lProdInstID);

    qry->Prepare(sSql) ;
    qry->Open();

    if (qry->Next()) {
        iValue = atol(qry->Field(0).AsString()) ;
    }
    qry->Close();

    if (rule.m_iProdInstType==iValue)
        return true;

    return false;
}

bool CommissionMgr::init()
{

    DEFINE_QUERY_EX(qry,"bill");
    //DEFINE_QUERY_EX(qry,"pub");
    char sSql[1024]={0};

    sprintf(sSql,
      "select rule_id, offer_id, prod_inst_type, commission*100,interval_month,total_times from commission_rule where state='10A' order by rule_id");

    qry->Prepare(sSql) ;
    qry->Open();
    CommissionRule data;

    while (qry->Next()) {
        data.m_iRuleID = qry->Field(0).AsLong();
        data.m_lOfferID = qry->Field(1).AsLong();
        data.m_iProdInstType = qry->Field(2).AsInteger();
        data.m_iCommission = qry->Field(3).AsInteger();
        data.m_iInterval = qry->Field(4).AsInteger();
        data.m_iTotalTimes = qry->Field(5).AsInteger();

        m_vecRule.push_back(data);
    }

    qry->Close();

    if (m_vecRule.empty()) {
        return false;
    }

    cout<< "Total effective rule: " << m_vecRule.size() << endl;


    sprintf(sSql,
      "select offer_inst_id, prod_inst_id, max(sett_times),max(sett_ym) from commission_inst group by offer_inst_id, prod_inst_id");

    qry->Prepare(sSql) ;
    qry->Open();
    CommissionInst inst;

    while (qry->Next()) {
        inst.m_lOfferInstID = qry->Field(0).AsLong();
        inst.m_lProdInstID = qry->Field(1).AsLong();
        inst.m_iSettTimes = qry->Field(2).AsInteger();
        inst.m_iSettYM = qry->Field(3).AsInteger();

        mapProdInstIndex.insert(ProdInstIndex::value_type(inst.m_lProdInstID, inst));
    }

    qry->Close();
    
    return true;
}

}

BMCO_SERVER_MAIN(BM35::CommissionMgr)
