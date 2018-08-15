package fill_view;

import java.sql.Connection;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
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
		options.addOption("eec", "ejecucion-estados-calamidad", true, "cargar ejecucion fisica y financiera de los  estados de calamidad");
		options.addOption("eec_gc", "actualizar-eventos-gc-est-calamidad", true, "actualiza eventos de guatecompras para un estado de calamidad");
		options.addOption("emp", "ejecucion-metas-presidenciales", false, "cargar ejecucion fisica y financiera de metas presidenciales");
		options.addOption(Option.builder("dt").hasArgs().longOpt("dimension-tiempo").desc("<ejercicio_inicio> <ejercicio_fin> crea la dimension tiempo").build());
		
		options.addOption("mp", "metas-presidenciales", false, "calcula la vista de metas presidenciales");
		options.addOption("mp_des", "metas-presidenciales-descentralizadas", false, "calcula la vista de metas presidenciales");
		options.addOption("efp", "ejecucion-financiera-prestamos", false, "cargar ejecucion financiera de Prestamos");
		options.addOption(Option.builder("ep").hasArgs().longOpt("ejecucion-presupuestaria").desc("cargar ejecucion presupuestaria").build());
		options.addOption("epl", "ejecucion-presupuestaria-load", true, "cargar de cache de ejecucion presupuestaria");
		options.addOption(Option.builder("eph").hasArgs().longOpt("ejecucion-presupuestaria-historia").desc("cargar historia ejecucion presupuestaria").build());
		options.addOption("ef", "ejecucion-fisica", true, "cargar ejecucion fisica");
		options.addOption("efh", "ejecucion-fisica-historia", true, "cargar historia ejecucion fisica");
		options.addOption("eff", "ejecucion-financiera-fisica", false, "actualiza vista de financiera-fisica");
		options.addOption("ei", "ejecucion-ingresos", false, "cargar ingresos");
		options.addOption("ing_ra", "ing_ra", true, "cargar ingresos recurso auxiliar por año");
		options.addOption("ing_r", "ing_r", true, "cargar ingresos recurso por año");
		options.addOption("sn", "snips", false, "cargar snips");
		options.addOption("egc", "eventos-guatecompras", true, "cargar eventos guatecompras");
		options.addOption("egch", "eventos-guatecompras-historia", true, "cargar historia de eventos guatecompras");
		options.addOption( "catalogos", "catalogos", true, "carga multiples catalogos" );
		options.addOption("deuda", "deuda", false, "actualiza vista de deuda");
		options.addOption("hospitales", "hospitales", false, "actualiza vista de hospitales");
		options.addOption("centros", "centros", false, "actualiza vista de centros");
		options.addOption("puestos", "puestos", false, "actualiza vista de puestos");
		options.addOption("epf", "ejecucion-finalidad", true, "cargar ejecucion presupuestaria por finalidad");
		options.addOption("eca", "ejecucion-contable-anticipo", true, "cargar ejecucion contable de anticipos");
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
				 Integer ejercicio = cline.getOptionValue("eec")!=null && cline.getOptionValue("eec").length()>0 ? 
						 Integer.parseInt(cline.getOptionValue("eec")) : 0;
				 if(     CEjecucionCalamidad.loadEjecucionFisicaFinanciera(false) && 
						 CEjecucionCalamidad.loadEjecucionFisicaFinanciera(true) &&
						 CEjecucionCalamidad.loadEjecucionFinancieraOtrosProgramas(ejercicio)
						 //CEjecucionCalamidad.loadActividadesPresupuestarias(false, 94) &&
						 //CEjecucionCalamidad.loadActividadesPresupuestarias(true, 94)
					)
					 CLogger.writeConsole("Datos de calamidad importadas con exito");
			 }
			 else if(cline.hasOption("ejecucion-fisica-calamidad")){
				 CLogger.writeConsole("Inicio registro avance fisico de estados de calamidad...");
				 if(CEjecucionCalamidad.loadEjecucionFisicaFinanciera(false)&&CEjecucionCalamidad.loadEjecucionFisicaFinanciera(true))
					 CLogger.writeConsole("Datos de metas fisicas calamidad importadas con exito");
			 }
			 else if(cline.hasOption("actualizar-eventos-gc-est-calamidad")) {
				 CLogger.writeConsole("Inicio de actualizacion de eventos guatecompras para el estado de calamidad para el subprograma indicado");
				 Integer subprograma = cline.getOptionValue("eec_gc")!=null && cline.getOptionValue("eec_gc").length()>0 ? 
						 Integer.parseInt(cline.getOptionValue("eec_gc")) : 0;
				 if(CEjecucionCalamidad.actualizarEventosGuatecompras(subprograma))
					 CLogger.writeConsole("Eventos de Guatecompras del estado de calamidad en el subprograma actualizados con éxito");
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
						CEjecucionFisica.loadEjeucionHoja(conn, false, false) &&
						CEjecucionFisica.loadEjecucionDetalle(conn, false, false) &&
						CUnidadMedida.loadUnidadesMedida(conn, false, false) &&
						CEjecucionPrestamos.loadEjecucionFinanciera()
					)
					CLogger.writeConsole("todas las tablas importadas con exito");
			 }
			 else if(cline.hasOption("dimension-tiempo")){
				 String[] argumentos = cline.getOptionValues("dt");
				 Integer ejercicio_inicio = argumentos!=null && argumentos.length>0 ? 
						 Integer.parseInt(argumentos[0]) : start.getYear();
				 Integer ejercicio_fin = argumentos!=null && argumentos.length>1 ? 
								 Integer.parseInt(argumentos[1]) : start.getYear();
				 CDimensionTiempo.createDimension(conn, ejercicio_inicio, ejercicio_fin);
				 CLogger.writeConsole("Se ha creado la dimension tiempo");
			 }
			 else if(cline.hasOption("ejecucion-presupuestaria")){
				 CLogger.writeConsole("Inicio carga de ejecucion presupuestaria");
				 String[] argumentos = cline.getOptionValues("ep");
				 Integer ejercicio =  argumentos!=null && argumentos.length>0 ? 
						 Integer.parseInt(argumentos[0]) : start.getYear();
				 boolean con_historia = argumentos!=null && argumentos.length>1 ? argumentos[1]=="true" : true;		 
				 if(CEjecucionPresupuestaria.loadEjecucionPresupuestaria(conn, ejercicio, true, con_historia))
					 CLogger.writeConsole("Datos de ejecucion presupuestaria cargados con exito");
			 }
			 else if(cline.hasOption("ejecucion-presupuestaria-load")){
				 CLogger.writeConsole("Inicio carga de cache de ejecucion presupuestaria");
				 Integer ejercicio = cline.getOptionValue("epl")!=null && cline.getOptionValue("epl").length()>0 ? 
						 Integer.parseInt(cline.getOptionValue("epl")) : start.getYear();
				 if(CEjecucionPresupuestaria.loadEjecucionPresupuestaria(conn, ejercicio, false, false))
					 CLogger.writeConsole("Datos de ejecucion presupuestaria cargados con exito");
			 }
			 else if(cline.hasOption("ejecucion-presupuestaria-historia")){
				 CLogger.writeConsole("Inicio carga de historia de ejecucion presupuestaria");
				 String[] argumentos = cline.getOptionValues("eph");
				 Integer ejercicio_inicio = argumentos!=null && argumentos.length>0 ? 
						 Integer.parseInt(argumentos[0]) : start.getYear();
				 Integer ejercicio_fin = argumentos!=null && argumentos.length>1 ? 
								 Integer.parseInt(argumentos[1]) : start.getYear();
				 if(CEjecucionPresupuestaria.loadEjecucionPresupuestariaHistoria(conn, ejercicio_inicio, ejercicio_fin))
					 CLogger.writeConsole("Datos Historicos de ejecucion presupuestaria cargados con exito");
			 }
			 else if(cline.hasOption("ejecucion-fisica")){
				 CLogger.writeConsole("Inicio carga de ejecucion fisica");
				 Integer ejercicio = cline.getOptionValue("ef")!=null && cline.getOptionValue("ef").length()>0 ? 
						 Integer.parseInt(cline.getOptionValue("ef")) : start.getYear();
				 if(CEjecucionFisica.loadEjeucionFisica(conn, ejercicio))
					 CLogger.writeConsole("Datos de ejecucion fisica cargados con exito");
			 }
			 else if(cline.hasOption("ejecucion-fisica-historia")){
				 CLogger.writeConsole("Inicio carga de historia de ejecucion fisica");
				 String[] argumentos = cline.getOptionValues("efh");
				 Integer ejercicio_inicio = argumentos!=null && argumentos.length>0 ? 
						 Integer.parseInt(argumentos[0]) : start.getYear();
				 Integer ejercicio_fin = argumentos!=null && argumentos.length>1 ? 
								 Integer.parseInt(argumentos[1]) : ejercicio_inicio;
				 if(CEjecucionFisica.loadEjeucionFisicaHistoria(conn, ejercicio_inicio, ejercicio_fin))
					 CLogger.writeConsole("Datos Historicos de ejecucion fisica cargados con exito");
			 }
			 else if(cline.hasOption("ejecucion-financiera-fisica")){
				 CLogger.writeConsole("Inicio de actualización de la vista financiera-fisica");
				 if(CEjecucionFinacieraFisica.updateFinancieraFisica(conn))
					 CLogger.writeConsole("Vista financiera-fisica actualizada con exito");
			 }
			 else if(cline.hasOption("deuda")){
				 CLogger.writeConsole("Inicio de actualización de la vista mv_deuda");
				 if(CEjecucionFinacieraFisica.updateDeuda(conn))
					 CLogger.writeConsole("Vista mv_deuda actualizada con exito");
			 }
			 else if(cline.hasOption("ejecucion-ingresos")){
				 CLogger.writeConsole("Inicio carga de ingresos");
				 Integer ejercicio = cline.getOptionValue("ei")!=null && cline.getOptionValue("ei").length()>0 ? 
						 Integer.parseInt(cline.getOptionValue("ei")) : start.getYear();
				 if(CIngreso.loadIngresos(conn, ejercicio))
					 CLogger.writeConsole("Datos de ingresos cargados con exito");
			 }
			 else if(cline.hasOption("snips")){
				 CLogger.writeConsole("Inicio carga de snips");
				 if(CSnip.loadSnip(conn))
					 CLogger.writeConsole("Datos de snips cargados con exito");
			 }
			 else if(cline.hasOption("eventos-guatecompras")){
				 CLogger.writeConsole("Inicio carga de eventos de Guatecompras");
				 Integer ejercicio = cline.getOptionValue("egc")!=null && cline.getOptionValue("egc").length()>0 ? 
						 Integer.parseInt(cline.getOptionValue("egc")) : start.getYear();
				 if(CEventoGuatecompras.loadEventosGC(conn, ejercicio))
					 CLogger.writeConsole("Datos de eventos de Guatecompras cargados con exito");
			 }
			 else if(cline.hasOption("eventos-guatecompras-historia")){
				 CLogger.writeConsole("Inicio carga historia de eventos de Guatecompras");
				 String[] argumentos = cline.getOptionValues("egch");
				 Integer ejercicio_inicio = argumentos!=null && argumentos.length>0 ? 
						 Integer.parseInt(argumentos[0]) : start.getYear();
				 Integer ejercicio_fin = argumentos!=null && argumentos.length>1 ? 
								 Integer.parseInt(argumentos[1]) : start.getYear();
				 if(CEventoGuatecompras.loadEventosGCHistoria(conn, ejercicio_inicio, ejercicio_fin))
					 CLogger.writeConsole("Datos historia de eventos de Guatecompras cargados con exito");
			 }
			 else if(cline.hasOption("ing_ra")){
				 CLogger.writeConsole("Inicio carga de ingresos por recurso y auxiliar");
				 Integer ejercicio = cline.getOptionValue("ing_ra")!=null && cline.getOptionValue("ing_ra").length()>0 ? 
						 Integer.parseInt(cline.getOptionValue("ing_ra")) : start.getYear();
				 if(CIngreso.loadIngresosRecursoAuxiliar(conn, ejercicio))
					 CLogger.writeConsole("Datos de ingresos por recurso y auxiliar cargados con exito");
			 }
			 else if(cline.hasOption("ing_r")){
				 CLogger.writeConsole("Inicio carga de ingresos por recurso");
				 Integer ejercicio = cline.getOptionValue("ing_r")!=null && cline.getOptionValue("ing_r").length()>0 ? 
						 Integer.parseInt(cline.getOptionValue("ing_r")) : start.getYear();
				 if(CIngreso.loadIngresosRecurso(conn, ejercicio))
					 CLogger.writeConsole("Datos de ingresos por recurso cargados con exito");
			 }
			 else if(cline.hasOption("catalogos")){
				 CLogger.writeConsole("Inicio carga de catalogos");
				 Integer ejercicio = cline.getOptionValue("catalogos")!=null && cline.getOptionValue("catalogos").length()>0 ? 
						 Integer.parseInt(cline.getOptionValue("catalogos")) : start.getYear();
				 if(CCatalogo.loadCatalogos(conn,ejercicio))
					 CLogger.writeConsole("Datos de catalogos cargados con éxito");
			 }
			 else if(cline.hasOption("hospitales")){
				 CLogger.writeConsole("Inicio de actualización de la vista mv_hospitales");
				 if(CSalud.updateHospitales(conn))
					 CLogger.writeConsole("Vista mv_hospitales actualizada con exito");
			 }
			 else if(cline.hasOption("centros")){
				 CLogger.writeConsole("Inicio de actualización de la vista mv_centros_salud");
				 if(CSalud.updateCentros(conn))
					 CLogger.writeConsole("Vista mv_centros_salud actualizada con exito");
			 }
			 else if(cline.hasOption("puestos")){
				 CLogger.writeConsole("Inicio de actualización de la vista mv_puestos_salud");
				 if(CSalud.updatePuestos(conn))
					 CLogger.writeConsole("Vista mv_puestos_salud actualizada con exito");
			 }
			 else if(cline.hasOption("ejecucion-finalidad")) {
				 CLogger.writeConsole("Inicio carga de ejecucion presupuestaria por finalidad");
				 Integer ejercicio = cline.getOptionValue("ejecucion-finalidad")!=null && cline.getOptionValue("ejecucion-finalidad").length()>0 ? 
						 Integer.parseInt(cline.getOptionValue("ejecucion-finalidad")) : start.getYear();
				 if(CEjecucionPresupuestaria.loadEjecucionPresupuestariaFinalidadFuncionDivision(conn,ejercicio))
					 CLogger.writeConsole("Vista de mv_ejecucion_presupuestaria_finalidad actualizada con éxito");
			 }
			 else if(cline.hasOption("ejecucion-contable-anticipo")) {
				 CLogger.writeConsole("Inicio carga de ejecucion contable de anticipos");
				 Integer ejercicio = cline.getOptionValue("ejecucion-contable-anticipo")!=null && cline.getOptionValue("ejecucion-contable-anticipo").length()>0 ? 
						 Integer.parseInt(cline.getOptionValue("ejecucion-contable-anticipo")) : start.getYear();
				 if(CEjecucionContable.loadEjecucionAnticipos(conn,ejercicio))
					 CLogger.writeConsole("Vista de mv_anticipo_contable actualizada con éxito");
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