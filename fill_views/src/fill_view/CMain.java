package fill_view;

import java.sql.Connection;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.joda.time.DateTime;
import org.joda.time.Minutes;
import org.joda.time.Seconds;

import utilities.CDimensionTiempo;
import utilities.CLogger;

public class CMain {
	private static Options options;
	
	static{
		options = new Options();
		options.addOption("tn_ejes", "tn-ejes", false, "calcula los datos de los ejes del TN");
		options.addOption("tn_ent", "tn-entidades", false, "calcula los datos de las entidades del TN");
		options.addOption("tn_ef", "tn-estructuras-financieras", false, "calcula las estructuras financieras del TN");
		options.addOption("update_all","update-all",false,"Cargar todas las tablas a MemSQL");
		options.addOption("eec", "ejecucion-estados-calamidad", false, "cargar ejecucion fisica y financiera de los  estados de calamidad");
		options.addOption("emp", "ejecucion-metas-presidenciales", false, "cargar ejecucion fisica y financiera de metas presidenciales");
		options.addOption("dt", "dimension-tiempo", false, "crea la dimension tiempo");
		options.addOption("mp", "metas-presidenciales", false, "calcula la vista de metas presidenciales");
		options.addOption("mp_des", "metas-presidenciales-descentralizadas", false, "calcula la vista de metas presidenciales");
		options.addOption("efp", "ejecucion-financiera-prestamos", false, "cargar ejecucion financiera de Prestamos");
		options.addOption("ep", "ejecucion-presupuestaria", false, "cargar ejecucion presupuestaria");
		options.addOption("ef", "ejecucion-fisica", false, "cargar ejecucion fisica");
		options.addOption( "h", "help", false, "muestra este listado de opciones" );
	}
	
	final static  CommandLineParser parser = new DefaultParser();
	
	 public static void main(String[] args) throws Exception {
		 DateTime start = new DateTime();
		 CommandLine cline = parser.parse( options, args );
		 if (CHive.connect()){
			 Connection conn = CHive.getConnection();
			 if(cline.hasOption("tn-ejes")){
				 CLogger.writeConsole("Inicio calculos financieros de ejes del triangulo norte...");
				 if(CTrianguloNorte.loadEjesTrianguloNorte())
					 CLogger.writeConsole("Datos de Ejes del Triangulo Norte, calculados con exito");
			 }
			 else if(cline.hasOption("tn-entidades")){
				 CLogger.writeConsole("Inicio calculos financieros de entidades del triangulo norte...");
				 if(CTrianguloNorte.loadEntidadesTrianguloNorte())
					 CLogger.writeConsole("Datos de Entidades del Triangulo Norte, calculados con exito");
			 }
			 else if(cline.hasOption("tn-estructuras-financieras")){
				 CLogger.writeConsole("Inicio calculos financieros de estructuras de financiamiento del triangulo norte...");
				 if(CTrianguloNorte.loadEstructurasFinanciamiento())
					 CLogger.writeConsole("Datos de Estructuras de financiamiento del Triangulo Norte, calculados con exito");
			 }
			 else if(cline.hasOption("ejecucion-financiera-calamidad")){
				 CLogger.writeConsole("Inicio registro financiero de estados de calamidad...");
				 if(CEjecucionCalamidad.loadEjecucionFisicaFinanciera(false) && CEjecucionCalamidad.loadEjecucionFisicaFinanciera(true))
					 CLogger.writeConsole("Datos de ejecucion finaciera calamidad importadas con exito");
			} 
			else if(cline.hasOption("ejecucion-estados-calamidad")){
				 CLogger.writeConsole("Inicio registro avance fisico y financiero de estados de calamidad...");
				 if(     CEjecucionCalamidad.loadEjecucionFisicaFinanciera(false) && 
						 CEjecucionCalamidad.loadEjecucionFisicaFinanciera(true) &&
						 CEjecucionCalamidad.loadActividadesPresupuestarias(false, 94) &&
						 CEjecucionCalamidad.loadActividadesPresupuestarias(true, 94))
					 CLogger.writeConsole("Datos de calamidad importadas con exito");
			 }
			 else if(cline.hasOption("ejecucion-fisica-calamidad")){
				 CLogger.writeConsole("Inicio registro avance fisico de estados de calamidad...");
				 if(CEjecucionCalamidad.loadEjecucionFisicaFinanciera(false)&&CEjecucionCalamidad.loadEjecucionFisicaFinanciera(true))
					 CLogger.writeConsole("Datos de metas fisicas calamidad importadas con exito");
			 }
			 else if(cline.hasOption("ejecucion-metas-presidenciales")){
				 CLogger.writeConsole("Inicio registro avance fisico y financiero de metas presidenciales...");
				 if( CMetaPresidencial.loadEjecucionFisicaFinanciera(false) && CMetaPresidencial.loadEjecucionFisicaFinanciera(true))
					 CLogger.writeConsole("Datos de metas fsicias y financieras presidenciales");
			 }
			 else if(cline.hasOption("metas-presidenciales")){
				 CLogger.writeConsole("Inicio calculo metas presidenciales...");
				 if(CMeta.calcularMv_meta_presidencial(conn, start.getYear()))
					 CLogger.writeConsole("Datos de metas presidenciales calculados con exito");
			 }
			 else if(cline.hasOption("metas-presidenciales-descentralizadas")){
				 CLogger.writeConsole("Inicio calculo metas presidenciales descentralizadas...");
				 if(CMeta.calcularMv_meta_presidencial_descentralizadas(conn, start.getYear()))
					 CLogger.writeConsole("Datos de metas presidenciales descentralizadas calculados con exito");
			 }
			 else if(cline.hasOption("ejecucion-financiera-prestamos")){
				 CLogger.writeConsole("Inicio carga de ejecucion financiera de los prestamos");
				 if(CEjecucionPrestamos.loadEjecucionFinanciera())
					 CLogger.writeConsole("Datos de prestamos cargados con exito");
			 }else if (cline.hasOption("update-all")){
				 CLogger.writeConsole("Inicio de importacion de todos las tablas.");
				 if(	CFechaActualizacionData.UpdateLoadDate("ejecucionpresupuestaria") &&
						CTrianguloNorte.loadEjesTrianguloNorte() &&
						CTrianguloNorte.loadEntidadesTrianguloNorte() &&
						CTrianguloNorte.loadEstructurasFinanciamiento() &&
						CFechaActualizacionData.UpdateLoadDate("paptn_ejecucionfinanciera") && 
						CEjecucionFisica.loadEjeucionHoja(conn, false, false) &&
						CEjecucionFisica.loadEjecucionDetalle(conn, false, false) &&
						CUnidadMedida.loadUnidadesMedida(conn, false, false) &&
						CEjecucionCalamidad.loadEjecucionFisicaFinanciera(false)&&CEjecucionCalamidad.loadEjecucionFisicaFinanciera(true) &&
						CEjecucionPrestamos.loadEjecucionFinanciera() 
					)
					CLogger.writeConsole("todas las tablas importadas con exito");
			 }
			 else if(cline.hasOption("dimension-tiempo")){
				 CDimensionTiempo.createDimension(conn, 2011, (new DateTime()).getYear());
				 CLogger.writeConsole("Se ha creado la dimension tiempo");
			 }
			 else if(cline.hasOption("ejecucion-presupuestaria")){
				 CLogger.writeConsole("Inicio carga de ejecucion presupuestaria");
				 if(CEjecucionPresupuestaria.loadEjecucionPresupuestaria(conn))
					 CLogger.writeConsole("Datos de ejecucion presupuestaria cargados con exito");
			 }
			 else if(cline.hasOption("ejecucion-fisica")){
				 CLogger.writeConsole("Inicio carga de ejecucion fisica");
				 if(CEjecucionFisica.loadEjeucionFisica(conn))
					 CLogger.writeConsole("Datos de ejecucion fisica cargados con exito");
			 }
			 else if(cline.hasOption("help")){
				 HelpFormatter formater = new HelpFormatter();
				 formater.printHelp(80,"Utilitario para carga de informacion a MemSQL", "", options,"");
				 System.exit(0);
			 }
			 if(!cline.hasOption("help")){
				 DateTime now = new DateTime();
				 CLogger.writeConsole("Tiempo total: " + Minutes.minutesBetween(start, now).getMinutes() + " minutos " + (Seconds.secondsBetween(start, now).getSeconds() % 60) + " segundos " +
				 (now.getMillis()%10) + " milisegundos ");
			 }
			 CHive.close(conn);
		 }
	 }			 
}