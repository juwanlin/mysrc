#ifndef _COMMISSION_MGR_H_
#define _COMMISSION_MGR_H_

#include "Bmco/Util/AbstractConfiguration.h"
#include "ProcessTools.h"
#include "Bmco/Util/HelpFormatter.h"
#include "ProcessTools.h"
#include <fstream>
#include <map>

//using namespace std;
//ad  

namespace BM35 {

	class CommissionRule {
	public:
		int m_iRuleID;
		long m_lOfferID;
		int m_iProdInstType;
		int m_iCommission;
		int m_iInterval;
		int m_iTotalTimes;
	};

	class CommissionInst {
	public:
		long m_lOfferInstID;
		long m_lProdInstID;
		int m_iSettTimes;
		int m_iSettYM;
	};
	
	class CommissionMgr:
		public ServerProcessTools
	{

	public:
		void defineOptions(Bmco::Util::OptionSet& options);
		void handleConfig(const std::string& name, const std::string& value);
	public:
		CommissionMgr() {};
		~CommissionMgr() {};

	protected:
		void argumentDefine(const std::string& name, const std::string& value);
		void initialize(Bmco::Util::Application& self);
		void uninitialize();

		int main(const std::vector<std::string>& args);

		void displayArgument(void);
		void displayHelp() {
			Bmco::Util::HelpFormatter helpFormatter(options());
			helpFormatter.setCommand(commandName());
			helpFormatter.setUsage("OPTIONS");
			helpFormatter.setHeader("HLA format process");
			helpFormatter.format(std::cout);
		};
	public:		
		bool init();
		
		bool check(CommissionRule rule, CommissionInst &inst);
		void calcCommission(CommissionRule rule);
		void resultInDB(CommissionRule rule);
		
		vector<CommissionRule> m_vecRule;
		vector<CommissionInst> m_vecInst;
		
		typedef std::map<long, CommissionInst> ProdInstIndex;
		ProdInstIndex mapProdInstIndex;
			
	};

}

#endif


