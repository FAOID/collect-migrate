package org.fao.openforis.collect.migrate;

import java.io.FileWriter;
import java.util.List;

import org.openforis.collect.model.CollectRecord;
import org.openforis.collect.model.CollectRecord.Step;
import org.openforis.collect.model.CollectSurvey;
import org.openforis.collect.persistence.RecordDao;
import org.openforis.collect.persistence.SurveyDao;
import org.openforis.idm.metamodel.EntityDefinition;
import org.openforis.idm.model.expression.InvalidExpressionException;
import org.openforis.idm.transform.AutomaticColumnProvider;
import org.openforis.idm.transform.ColumnProvider;
import org.openforis.idm.transform.ColumnProviderChain;
import org.openforis.idm.transform.DataTransformation;
import org.openforis.idm.transform.PivotExpressionColumnProvider;
import org.openforis.idm.transform.SingleAttributeColumnProvider;
import org.openforis.idm.transform.csv.ModelCsvWriter;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.openforis.idm.metamodel.Schema;


/**
 * 
 * @author G. Miceli
 * @author E. Wibowo
 *
 */
public class NaformaExtractor {
	private RecordDao recordDao;
	private CollectSurvey survey;

	public NaformaExtractor(RecordDao recordDao, CollectSurvey survey) {
		this.recordDao = recordDao;
		this.survey = survey;
	}

	public DataTransformation getNaturalForest() throws InvalidExpressionException {
		Schema schema = survey.getSchema();
		String axisPath = "/cluster/natural_forest";
		EntityDefinition rowDefn = (EntityDefinition) schema.getByPath(axisPath);
		ColumnProvider provider = new ColumnProviderChain(
				new PivotExpressionColumnProvider("/",//parent() 
					new SingleAttributeColumnProvider("tract_no", "Tract"),
					new SingleAttributeColumnProvider("subplot_no", "Subplot")),
				new AutomaticColumnProvider(rowDefn));
		return new DataTransformation(axisPath, provider);
	}
	
	/*public DataTransformation getHouseholdTransform() throws InvalidExpressionException {
		Schema schema = survey.getSchema();
		String axisPath = "/cluster/household";
		EntityDefinition rowDefn = (EntityDefinition) schema.getByPath(axisPath);
		ColumnProvider provider = new ColumnProviderChain(
				new PivotExpressionColumnProvider("parent()", 
					new SingleAttributeColumnProvider("id", "cluster_id"),
					new SingleAttributeColumnProvider("region", "cluster_region"),
					new SingleAttributeColumnProvider("district", "cluster_district")),
				new AutomaticColumnProvider(rowDefn));
		return new DataTransformation(axisPath, provider);
	}

	public DataTransformation getForestProductTransform() throws InvalidExpressionException {
		Schema schema = survey.getSchema();
		String axisPath = "/cluster/household/product_used";
		EntityDefinition rowDefn = (EntityDefinition) schema.getByPath(axisPath);
		ColumnProvider provider = new ColumnProviderChain(
				new PivotExpressionColumnProvider("parent()/parent()", 
					new SingleAttributeColumnProvider("id", "cluster_id"),
					new SingleAttributeColumnProvider("region", "cluster_region"),
					new SingleAttributeColumnProvider("district", "cluster_district")),
				new PivotExpressionColumnProvider("parent()", 
						new SingleAttributeColumnProvider("id", "household_id")),
				new AutomaticColumnProvider(rowDefn));
		return new DataTransformation(axisPath, provider);
	}*/

	/**
	 * @param dataPath
	 * @param columns2 
	 * @throws Exception
	 */
	
	public void extractData(String outfile, DataTransformation xform) throws Exception {
		
		ModelCsvWriter out = null;
		try {
			out = new ModelCsvWriter(new FileWriter(outfile), xform);
			out.printColumnHeadings();
			
			// Cycle over data files
			int rowsCount = 0;
			long read = 0;
			long start = System.currentTimeMillis();
			List<CollectRecord> summaries = recordDao.loadSummaries(survey, "cluster", 0, Integer.MAX_VALUE, null, null);
			for (CollectRecord s : summaries) {
				if ( s.getStep() == Step.ENTRY ) {
					CollectRecord record = recordDao.load(survey, s.getId(), 1);
					rowsCount += out.printData(record);
					read++;
				}
			}
			long duration = System.currentTimeMillis()-start;
			if(rowsCount>0)
			{
				System.out.println("Exported "+rowsCount+" rows from "+read+" records in "+(duration/1000)+"s ("+(duration/rowsCount)+"ms/row).");
			}else{
				System.out.println("No data found");
			}
			
		} finally {
			if ( out!=null ) {
				out.flush();
				out.close();
			}
		}
	}


	public static void main(String[] args) throws Throwable {
		String surveyName = "idnfi";
		
		ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("migrate-context.xml");
		SurveyDao surveyDao = ctx.getBean(SurveyDao.class);
		RecordDao recordDao = ctx.getBean(RecordDao.class);
		CollectSurvey survey = surveyDao.load(surveyName);
		NaformaExtractor naformaExtractor = new NaformaExtractor(recordDao, survey);

		// Import informant interviews		
		DataTransformation xform1 = naformaExtractor.getNaturalForest();
		naformaExtractor.extractData("E:\\data\\export\\natural_forest.csv",xform1);
	}
}
