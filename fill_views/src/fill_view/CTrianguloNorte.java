package fill_view;
import java.sql.PreparedStatement;
import org.joda.time.DateTime;
import utilities.CLogger;

public class CTrianguloNorte {
	public static boolean loadEntidadesTrianguloNorte(){
	
		boolean ret = false;
		try{
			if( CMemSQL.connect()){
				ret = true;

				CLogger.writeConsole("CTrianguloNorte Entidades:");
				PreparedStatement pstm = CMemSQL.getConnection().prepareStatement("DELETE FROM mv_paptn_tablaentidades WHERE mes = month(CURRENT_TIMESTAMP) ");
				if (pstm.executeUpdate()>0)
					CLogger.writeConsole("Registros eliminados");
				else
					CLogger.writeConsole("Sin registros para eliminar");
				pstm.close();
				DateTime date = new DateTime();
					pstm = CMemSQL.getConnection().prepareStatement("INSERT INTO mv_paptn_tablaentidades(mes, "+
                    "entidad, "+
                    "nombre, "+
                    "gp_ejecucion, "+
                    "gp_vigente, "+
                    "gp_porcentaje, "+
					"paptn_ejecucion, "+
					"paptn_vigente, "+
					"paptn_porcentaje) "+
					"select month(CURRENT_TIMESTAMP) mes,t3.entidad, t3.nombre, gp_ejecucion, gp_vigente, gp_porcentaje, paptn_ejecucion, paptn_vigente, paptn_porcentaje "+
					"from ( "+
					"	SELECT t1.*, "+
					"            t2.vigente gp_vigente, "+
					"            ((t1.gp_ejecucion) / (t2.vigente) * 100) gp_porcentaje "+
					"       FROM (  SELECT e.entidad, e.nombre, sum(g.monto_renglon) gp_ejecucion "+
					"                 FROM cg_entidades e "+
					"                      LEFT OUTER JOIN eg_gasto g "+
					"                         ON (    e.entidad = g.entidad "+
					"                             AND e.ejercicio = g.ejercicio "+
					"                             AND g.mes <= month(CURRENT_TIMESTAMP)"+
					"							  AND g.iclase_registro>0 ) "+
					"                WHERE     e.ejercicio = year(CURRENT_TIMESTAMP) "+
					"                      AND (   (e.entidad BETWEEN 11130000 AND 11130020) "+
					"                           OR (e.entidad = 11140021)) "+
					"             GROUP BY e.entidad, e.nombre, g.entidad) t1 "+
					"            LEFT OUTER JOIN "+
					"            (  SELECT entidad, sum(vigente_"+date.getMonthOfYear()+") vigente, sum(asignado) asignado "+
					"                 FROM vigente v "+
					"                WHERE ejercicio = year(CURRENT_TIMESTAMP) "+
					"             GROUP BY entidad) t2 "+
					"               ON t1.entidad = t2.entidad "+
					") t3 left outer join "+
					"(select t1.*, t2.vigente paptn_vigente, (t1.paptn_ejecucion/t2.vigente) paptn_porcentaje "+ 
					"from ( "+ 
					"select ep.entidad, sum(g.monto_renglon) paptn_ejecucion "+
					"from paptn_estructura_presupuestaria ep left outer join eg_gasto g "+
					"on (g.entidad = ep.entidad "+ 
					"and g.programa = ep.programa "+ 
					"and g.subprograma = ep.subprograma "+ 
					"and if(ep.proyecto is not null, g.proyecto = ep.proyecto,true) "+ 
					"and if(ep.obra is not null, g.obra = ep.obra, true) "+ 
					"and if(ep.actividad is not null, g.actividad = ep.actividad, true) "+ 
					"and ((ep.entidad = 11130008 and g.renglon>=100) OR (ep.entidad<>11130008) OR (ep.entidad=11130008 and ep.linea_accionid=5)) "+
					"and g.ejercicio = year(CURRENT_TIMESTAMP) and g.iclase_registro>0), "+
					"paptn_fuente f "+
					"where f.estructura_presupuestariaid = ep.id "+
					"and f.fuente = g.fuente "+
					"group by ep.entidad "+
					") t1 left outer join "+
					"( "+  
					"select ep.entidad, sum(v.vigente_"+date.getMonthOfYear()+") vigente "+
					"	from paptn_estructura_presupuestaria ep left outer join vigente v "+ 
					"	on (v.entidad = ep.entidad "+ 
					"and v.programa = ep.programa "+ 
					"and v.subprograma = ep.subprograma "+ 
					"and ((ep.entidad = 11130008 and v.renglon>=100) OR (ep.entidad<>11130008) OR (ep.entidad=11130008 and ep.linea_accionid=5)) "+
					"and if(ep.proyecto is not null, v.proyecto = ep.proyecto,true) "+ 
					"and if(ep.obra is not null, v.obra = ep.obra, true) "+ 
					"and if(ep.actividad is not null, v.actividad = ep.actividad, true) ), paptn_fuente f "+
					"	where ep.id = f.estructura_presupuestariaid "+
					"	and v.fuente = f.fuente "+
					"	group by ep.entidad "+
					") t2 on (t1.entidad = t2.entidad) ) t4 "+
					"on t3.entidad = t4.entidad "+
					"order by t3.entidad ");   
 
					ret = ret && pstm.executeUpdate()>0;
					pstm.close();
				}					
				
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 1: CTrianguloNorte.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
	
	public static boolean loadEjesTrianguloNorte(){
		DateTime date = new DateTime();
		String query = "INSERT INTO mv_paptn_tablaejes(mes,eje,linea,eje_nombre, eje_nombre_corto,linea_nombre,aprobado, vigente,modificaciones,ejecucion,porcentaje,iorder) " +
					    "SELECT MONTH(CURRENT_TIMESTAMP) AS MES, t3.ideje, t3.idlinea, t3.eje, t3.eje_corto, t3.linea, t3.asignado,t3.vigente, t3.modificaciones, t3.ejecutado, t3.porcentaje, t3.orden "+
						"from "+
						"(SELECT eje.iorder, "+
						"                  eje.id ideje, "+
						"                  NULL idlinea, "+
						"                  eje.nombre eje, "+
						"				   eje.nombre_corto eje_corto,"+		
						"                  NULL linea, "+
						"                  (eje.iorder * 10) orden, "+
						"				  0.0 asignado, "+
						"				  0.0 vigente, "+
						"				  0.0 modificaciones, "+
						"				  0.0 ejecutado, "+
						"				  0.0 porcentaje "+
						"            FROM paptn_eje_estrategico eje "+
						"UNION ALL "+
						"select l.iorder,l.eje_estrategicoid ideje, l.id idlinea,null eje, null eje_corto,l.nombre linea, (l.iorder + (10 * l.eje_estrategicoid)) orden, t1.asignado, t1.vigente, t1.modificaciones, t2.ejecutado, (t2.ejecutado/t1.vigente) porcentaje "+
						"from paptn_linea_accion l left outer join "+
						"(select e.linea_accionid, sum(v.asignado) asignado, sum(v.vigente_"+ date.getMonthOfYear() +") vigente, (sum(v.vigente_"+ date.getMonthOfYear() +")-sum(v.asignado)) modificaciones "+
						"	from minfin.paptn_estructura_presupuestaria e, paptn_fuente f,minfin.vigente v "+
						"	where e.entidad = v.entidad "+
						"	and e.programa = v.programa "+
						"	and e.subprograma = v.subprograma "+
						"	and if(e.proyecto is not null, e.proyecto = v.proyecto, true) "+
						"	and if(e.actividad is not null, e.actividad = v.actividad, true)  "+
						"	and if(e.obra is not null, e.obra=v.obra, true) "+
						"	and e.id = f.estructura_presupuestariaid "+
						"	and ((e.entidad = 11130008 and v.renglon>=100) OR (e.entidad<>11130008) OR (e.entidad=11130008 and e.linea_accionid=5))"+
						"	and v.fuente = f.fuente "+
						"	group by e.linea_accionid "+
						") t1 on (l.id = t1.linea_accionid) "+
						"left outer join "+
						"( "+
						"	select e.linea_accionid, sum(g.monto_renglon) ejecutado "+
						"	from paptn_estructura_presupuestaria e, paptn_fuente f, eg_gasto g "+
						"	where e.entidad = g.entidad "+
						"	and e.programa = g.programa "+
						"	and e.subprograma = g.subprograma "+
						"	and if(e.proyecto is not null, e.proyecto = g.proyecto, true) "+
						"	and if(e.actividad is not null, e.actividad = g.actividad, true)  "+
						"	and if(e.obra is not null, e.obra=g.obra, true) "+
						"	and g.mes <= month(CURRENT_TIMESTAMP) "+
						"	and g.ejercicio = year(CURRENT_TIMESTAMP) "+
						"	and g.iclase_registro>0 "+
						"	and f.estructura_presupuestariaid = e.id "+
						"	and ((e.entidad = 11130008 and g.renglon>=100) OR (e.entidad<>11130008) OR (e.entidad=11130008 and e.linea_accionid=5))"+
						"	and g.fuente = f.fuente "+
						"	group by e.linea_accionid "+
						") t2 on (t1.linea_accionid=t2.linea_accionid and l.id = t1.linea_accionid and l.id = t2.linea_accionid) "+
						") t3 "+
						"order by orden ";
		
		boolean ret = false;
		try{
			if( CMemSQL.connect()){
				ret = true;
				CLogger.writeConsole("CTrianguloNorte Ejes:");
				PreparedStatement pstm = CMemSQL.getConnection().prepareStatement("DELETE FROM mv_paptn_tablaejes where mes = month(CURRENT_TIMESTAMP) ");
				if (pstm.executeUpdate()>0)
					CLogger.writeConsole("Registros eliminados");
				else
					CLogger.writeConsole("Sin registros para eliminar");
				pstm.close();
				pstm = CMemSQL.getConnection().prepareStatement(query);
				ret = ret && pstm.executeUpdate()>0;
				pstm.close();
			}
		
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 2: CTrianguloNorte.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}

	public static boolean loadEstructurasFinanciamiento() {
		boolean ret = true;
		try{
			if( CMemSQL.connect()){
				CLogger.writeConsole("CTrianguloNorte Estructuras de Financiamiento:");
				PreparedStatement pstm = CMemSQL.getConnection().prepareStatement("DELETE FROM mv_paptn_estructura_financiamiento WHERE ejercicio = YEAR(CURRENT_TIMESTAMP) ");
				if (pstm.executeUpdate()>0)
					CLogger.writeConsole("Registros eliminados");
				else
					CLogger.writeConsole("Sin registros para eliminar");
				pstm.close();
				DateTime date = new DateTime();
					pstm = CMemSQL.getConnection().prepareStatement("INSERT INTO mv_paptn_estructura_financiamiento(ejercicio, tributarias, prestamos_externos, donaciones, otras) "+
							"select v.ejercicio, " + 
							"sum(case when v.fuente in (11,21,22,29) then v.vigente_"+date.getMonthOfYear()+" else 0 end) tributarias, " + 
							"sum(case when v.fuente=52 then v.vigente_"+date.getMonthOfYear()+" else 0 end) prestamos_externos, " + 
							"sum(case when v.fuente=61 then v.vigente_"+date.getMonthOfYear()+" else 0 end) donaciones_externas, " + 
							"sum(case when (v.fuente not in(11,21,22,29) and v.fuente<>52 and v.fuente<>61) then v.vigente_"+date.getMonthOfYear()+" else 0 end) otras " + 
							"from paptn_estructura_presupuestaria e,vigente v, paptn_fuente f " + 
							"where f.estructura_presupuestariaid = e.id " + 
							"and e.entidad = v.entidad " + 
							"and e.programa = v.programa " + 
							"and e.subprograma = v.subprograma " + 
							"and f.fuente = v.fuente " + 
							"and ((e.entidad = 11130008 and v.renglon>=100) OR (e.entidad<>11130008) OR (e.entidad=11130008 and e.linea_accionid=5)) " + 
							"and if(e.proyecto IS NOT NULL, e.proyecto = v.proyecto,TRUE) " + 
							"and if(e.obra IS NOT NULL, e.obra = v.obra, TRUE) " + 
							"and if(e.actividad IS NOT NULL, e.actividad = v.actividad, TRUE) " + 
							"and v.ejercicio = "+date.getYear()+" "+
							"group by v.ejercicio");   
 					ret = ret && pstm.executeUpdate()>0;
					pstm.close();
				}					
				
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 3: CTrianguloNorte.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
}
