package fill_view;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import utilities.CLogger;

public class CEjecucionPresupuestaria {
	
	public static boolean loadEjecucionPresupuestaria(Connection conn){
		
		boolean ret = false;
		try{
			if( !conn.isClosed() && CMemSQL.connect()){
				ret = true;

				CLogger.writeConsole("CEjecucionPresupuestaria Entidades:");
				PreparedStatement pstm = conn.prepareStatement("TRUNCATE TABLE dashboard.mv_estructura");
				pstm.executeUpdate();
				pstm.close();
				pstm = conn.prepareStatement("TRUNCATE TABLE dashboard.mv_gasto");
				pstm.executeUpdate();
				pstm.close();
				pstm = conn.prepareStatement("TRUNCATE TABLE dashboard.mv_cuota");
				pstm.executeUpdate();
				pstm.close();
				pstm = conn.prepareStatement("TRUNCATE TABLE dashboard.mv_vigente");
				pstm.executeUpdate();
				pstm.close();
				pstm = conn.prepareStatement("TRUNCATE TABLE dashboard.mv_ejecucion_presupuestaria");
				pstm.executeUpdate();
				pstm.close();
				
				
				pstm = conn.prepareStatement("insert into table dashboard.mv_estructura "+
						"select e.ejercicio,t.mes, e.entidad, e.nombre entidad_nombre, ue.unidad_ejecutora, ue.nombre ue_nombre, "+
						"p.programa, p.nom_estructura programa_nombre, "+
						"sp.subprograma, sp.nom_estructura subprograma_nombre, "+
						"pr.proyecto, pr.nom_estructura proyecto_nombre, "+
						"ao.actividad, ao.obra, ao.nom_estructura actividad_obra_nombre "+
						"from sicoinprod.cg_entidades e, sicoinprod.cg_entidades ue, dashboard.mv_entidad mve, "+
						"sicoinprod.cp_estructuras p, sicoinprod.cp_estructuras sp, sicoinprod.cp_estructuras pr, sicoinprod.cp_estructuras ao, "+
						"dashboard.tiempo t "+
						"where e.ejercicio = year(current_date()) "+
						"and t.ejercicio = e.ejercicio "+
						"and t.dia = 1 "+
						"and ue.ejercicio = e.ejercicio "+
						"and e.restrictiva = 'N' "+
						"and ue.restrictiva = 'N' "+
						"and e.unidad_ejecutora = 0 "+
						"and ue.entidad = e.entidad "+
						"and ((e.entidad between 11130000 and 11130020) OR e.entidad = 11140021) "+ 
						"and mve.entidad = e.entidad "+
						"and mve.ejercicio = e.ejercicio "+
						"and ((ue.unidad_ejecutora = 0 and mve.unidades_ejecutoras=1) or (ue.unidad_ejecutora>0 and mve.unidades_ejecutoras>1)) "+
						"and p.ejercicio = e.ejercicio "+
						"and p.entidad = e.entidad "+
						"and p.unidad_ejecutora = ue.unidad_ejecutora "+
						"and p.nivel_estructura = 2 "+
						"and sp.ejercicio = e.ejercicio "+
						"and sp.entidad = e.entidad "+
						"and sp.unidad_ejecutora = ue.unidad_ejecutora "+
						"and sp.programa = p.programa "+
						"and sp.nivel_estructura = 3 "+
						"and pr.ejercicio = e.ejercicio "+
						"and pr.entidad = e.entidad "+
						"and pr.unidad_ejecutora = ue.unidad_ejecutora "+
						"and pr.programa = p.programa "+
						"and pr.subprograma = sp.subprograma "+
						"and pr.nivel_estructura = 4 "+
						"and ao.ejercicio = e.ejercicio "+
						"and ao.entidad = e.entidad "+
						"and ao.unidad_ejecutora = ue.unidad_ejecutora "+
						"and ao.programa = p.programa "+
						"and ao.subprograma = sp.subprograma "+
						"and ao.proyecto = pr.proyecto "+
						"and ao.nivel_estructura = 5");
				pstm.executeUpdate();
				pstm.close();
				
				///Actualiza la vista de gasto
				pstm = conn.prepareStatement("inser into table dashboard.mv_gasto "
						+"select t1.mes,t1.entidad, t1.unidad_ejecutora, t1.programa, t1.subprograma, t1.proyecto, t1.actividad, t1.obra, "+
						"t2.fuente, t2.grupo, t2.grupo_nombre, t2.subgrupo, t2.subgrupo_nombre, t2.renglon, t2.renglon_nombre, t2.ano_1, t2.ano_2, t2.ano_3, t2.ano_4, t2.ano_5, t2.ano_actual  "+
						"from dashboard.mv_estructura t1 left outer join ( "+
						"select gd.entidad, gd.unidad_ejecutora, gd.programa, gd.subprograma, gd.proyecto, gd.actividad, gd.obra, gd.renglon, r.nombre renglon_nombre, gd.fuente,   "+
						"						 	month(gh.fec_aprobado) mes, gd.renglon - (gd.renglon%100) grupo, gg.nombre grupo_nombre, gd.renglon - (gd.renglon%10) subgrupo, sg.nombre subgrupo_nombre,  "+  
						"						 	sum( case when gh.ejercicio = (year(current_date()) - 5) then gd.monto_renglon else 0 end) ano_1,    "+
						"						 	sum( case when gh.ejercicio = (year(current_date()) - 4) then gd.monto_renglon else 0 end) ano_2,    "+
						"						 	sum( case when gh.ejercicio = (year(current_date()) - 3) then gd.monto_renglon else 0 end) ano_3,    "+
						"						 	sum( case when gh.ejercicio = (year(current_date()) - 2) then gd.monto_renglon else 0 end) ano_4,    "+
						"						 	sum( case when gh.ejercicio = (year(current_date()) - 1) then gd.monto_renglon else 0 end) ano_5,    "+
						"						 	sum( case when gh.ejercicio = (year(current_date())) then gd.monto_renglon else 0 end) ano_actual    "+
						"						 				from sicoinprod.eg_gastos_hoja gh, sicoinprod.eg_gastos_detalle gd,    "+
						"										sicoinprod.cp_grupos_gasto gg, sicoinprod.cp_objetos_gasto sg, sicoinprod.cp_objetos_gasto r "+		
						"						 				where gh.ejercicio = gd.ejercicio       "+
						"						 				and gh.entidad = gd.entidad     "+
						"						 				and gh.unidad_ejecutora = gd.unidad_ejecutora    "+ 
						"						 				and gh.no_cur = gd.no_cur     "+
						"						 				and gh.clase_registro IN ('DEV', 'CYD', 'RDP', 'REG')   "+  
						"						 				and gh.estado = 'APROBADO'     "+
						"						 				and gh.ejercicio > (year(current_date())-6) "+    
						" 										and gg.ejercicio = year(current_date()) "+
		 				" 										and gg.grupo_gasto = (gd.renglon-(gd.renglon%100)) "+
		 				" 										and sg.ejercicio = year(current_date())  "+
		 				" 										and sg.renglon = (gd.renglon - (gd.renglon%10)) "+     
		 				" 										and r.ejercicio = year(current_date()) "+
		 				" 										and r.renglon = gd.renglon "+
						"						 				group by month(gh.fec_aprobado), gd.entidad, gd.unidad_ejecutora, gd.programa, gd.subprograma, "+    
						"						 				gd.proyecto, gd.actividad, gd.obra, gg.nombre, sg.nombre, r.nombre, gd.renglon, gd.fuente "+
						") t2 "+
						"on (t1.mes = t2.mes "+ 
						"	and t1.entidad = t2.entidad "+
						"	and t1.unidad_ejecutora = t2.unidad_ejecutora "+
						"	and t1.programa = t2.programa "+
						"	and t1.subprograma = t2.subprograma "+
						"	and t1.proyecto  = t2.proyecto "+
						"	and t1.actividad = t2.actividad "+
						"	and t1.obra = t2.obra "+
						")");
 
				pstm.executeUpdate();
				pstm.close();
					
				///Actualiza la vista de cuota
				pstm = conn.prepareStatement("INSERT INTO table dashboard.mv_cuota "+
						"SELECT d.ejercicio, "+ 
		                   "d.entidad, "+ 
		                   "d.unidad_ejecutora, "+ 
		                   "d.fuente, "+  
		                   "t.mes, "+
		                   "sum(case  "+
		                   	"when t.mes=1 and d.cuatrimestre=1 then cuota_mes1_sol "+
		                   	"when t.mes=2 and d.cuatrimestre=1 then cuota_mes2_sol "+
		                   	"when t.mes=3 and d.cuatrimestre=1 then cuota_mes3_sol "+
		                   	"when t.mes=4 and d.cuatrimestre=1 then cuota_mes4_sol "+
		                   	"when t.mes=5 and d.cuatrimestre=2 then cuota_mes1_sol "+
		                   	"when t.mes=6 and d.cuatrimestre=2 then cuota_mes2_sol "+
		                   	"when t.mes=7 and d.cuatrimestre=2 then cuota_mes3_sol "+
		                   	"when t.mes=8 and d.cuatrimestre=2 then cuota_mes4_sol "+
		                   	"when t.mes=9 and d.cuatrimestre=3 then cuota_mes1_sol "+
		                   	"when t.mes=10 and d.cuatrimestre=3 then cuota_mes2_sol "+
		                   	"when t.mes=11 and d.cuatrimestre=3 then cuota_mes3_sol "+
		                   	"when t.mes=12 and d.cuatrimestre=3 then cuota_mes4_sol "+
		                   "end) solicitado, "+  
		                   "sum(case  "+
		                   	"when t.mes=1 and d.cuatrimestre=1 then cuota_mes1_apr "+
		                   	"when t.mes=2 and d.cuatrimestre=1 then cuota_mes2_apr "+
		                   	"when t.mes=3 and d.cuatrimestre=1 then cuota_mes3_apr "+
		                   	"when t.mes=4 and d.cuatrimestre=1 then cuota_mes4_apr "+
		                   	"when t.mes=5 and d.cuatrimestre=2 then cuota_mes1_apr "+
		                   	"when t.mes=6 and d.cuatrimestre=2 then cuota_mes2_apr "+
		                   	"when t.mes=7 and d.cuatrimestre=2 then cuota_mes3_apr "+
		                   	"when t.mes=8 and d.cuatrimestre=2 then cuota_mes4_apr "+
		                   	"when t.mes=9 and d.cuatrimestre=3 then cuota_mes1_apr "+
		                   	"when t.mes=10 and d.cuatrimestre=3 then cuota_mes2_apr "+
		                   	"when t.mes=11 and d.cuatrimestre=3 then cuota_mes3_apr "+
		                   	"when t.mes=12 and d.cuatrimestre=3 then cuota_mes4_apr "+
		                   "end) aprobado "+
		                   "FROM sicoinprod.eg_financiero_detalle_4 D, "+
		                   "sicoinprod.eg_financiero_hoja_4 H1, dashboard.tiempo t "+ 
		                   "WHERE  h1.ejercicio = d.ejercicio "+  
		                   	"and d.ejercicio = t.ejercicio "+ 
		                    "AND h1.entidad = d.entidad "+
		                    "AND h1.unidad_ejecutora = d.unidad_ejecutora "+
		                    "AND h1.unidad_desconcentrada = d.unidad_desconcentrada "+
		                    "AND h1.no_cur = d.no_cur  "+
		                    "AND H1.CLASE_REGISTRO IN ('RPG', 'PRG', 'RPGI') "+ 
		                    "AND H1.estado = 'APROBADO' "+ 
		                    "AND t.dia = 1 "+
		                    "GROUP BY d.ejercicio, "+
		                    "d.entidad,  "+
		                    "d.unidad_ejecutora,  "+
		                    "d.fuente, "+ 
		                    "t.mes"
						);	
				pstm.executeUpdate();
				pstm.close();
				
				//Actualiza la vista de mv_vigente
				pstm = conn.prepareStatement("INSERT INTO TABLE dashboard.mv_vigente select p.ejercicio,p.entidad, p.unidad_ejecutora, p.fuente, p.renglon, p.programa, p.subprograma, p.proyecto, p.actividad, p.obra, t.mes, p.asignado, " + 
						"case " + 
						"               when t.mes=1 then vigente_1 " + 
						"               when t.mes=2 then vigente_2 " + 
						"               when t.mes=3 then vigente_3 " + 
						"               when t.mes=4 then vigente_4 " + 
						"               when t.mes=5 then vigente_5 " + 
						"               when t.mes=6 then vigente_6 " + 
						"               when t.mes=7 then vigente_7 " + 
						"               when t.mes=8 then vigente_8 " + 
						"               when t.mes=9 then vigente_9 " + 
						"               when t.mes=10 then vigente_10 " + 
						"               when t.mes=11 then vigente_11 " + 
						"               when t.mes=12 then vigente_12 " + 
						"end AS vigente " + 
						"from sicoinprod.vw_partidas p , dashboard.tiempo t " + 
						"where p.ejercicio= year(current_date()) " + 
						"and p.ejercicio=t.ejercicio  " + 
						"and t.dia=1");
				pstm.executeUpdate();
				pstm.close();
				
				//Actualiza la vista de mv_ejecucion_presupuestaria
				pstm = conn.prepareStatement("INSERT INTO TABLE dashboard.mv_ejecucion_presupuestaria "+
						"select e.*, g.fuente, g.grupo, g.grupo_nombre, g.subgrupo, g.subgrupo_nombre, g.renglon, g.renglon_nombre, g.ano_1, g.ano_2, g.ano_3, g.ano_4, g.ano_5, g.ano_actual, c.solicitado, c.aprobado, v.asignado, v.vigente "+
						"from mv_estructura e, mv_gasto g, mv_cuota c, mv_vigente v "+
						"where e.mes = g.mes  "+
						"and e.entidad = g.entidad "+
						"and e.unidad_ejecutora = g.unidad_ejecutora "+
						"and e.programa = g.programa "+
						"and e.subprograma = g.subprograma "+
						"and e.proyecto = g.proyecto "+
						"and e.actividad = g.actividad "+
						"and e.obra = g.obra "+
						"and g.entidad = c.entidad "+
						"and g.unidad_ejecutora = c.unidad_ejecutora "+
						"and g.fuente = c.fuente "+
						"and g.mes = c.mes "+
						"and v.entidad = e.entidad "+
						"and v.unidad_ejecutora = e.unidad_ejecutora "+
						"and v.programa = e.programa "+
						"and v.subprograma = e.subprograma "+
						"and v.proyecto = e.proyecto "+
						"and v.actividad = e.actividad "+
						"and v.obra = e.obra "+
						"and v.renglon = g.renglon "+
						"and v.fuente = g.fuente "+
						"and v.mes = e.mes");
				pstm.executeUpdate();
				pstm.close();
				
				ResultSet rs = conn.prepareStatement("SELECT * FROM dashboard.mv_ejecucion_presupuestaria").executeQuery();
				boolean bconn =  CMemSQL.connect();
				if(rs!=null && bconn){
					ret = true;
					int rows = 0;
					boolean first=true;
					while(rs.next()){
						if(first){
							pstm = CMemSQL.getConnection().prepareStatement("delete from mv_ejecucion_presupuestaria ");
							if (pstm.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");	
							pstm.close();
							first=false;
						}
						pstm = CMemSQL.getConnection().prepareStatement("Insert INTO mv_ejecucion_presupuestaria(ejercicio, mes, entidad, entidad_nombre, unidad_ejecutora, ue_nombre, programa, programa_nombre, subprograma, subprograma_nombre, " + 
								"proyecto, proyecto_nombre, actividad, obra, act_obra_nombre, renglon, renglon_nombre, subgrupo, subgrupo_nombre, grupo," + 
								"grupo_nombre, fuente, ano_1, ano_2, ano_3, ano_4, ano_5, ano_actual, solicitado, aprobado, asignado, vigente) "
								+ "values (?,?,?,?,?,?,?,?,?,?,"
								+ "?,?,?,?,?,?,?,?,?,?,"
								+ "?,?,?,?,?,?,?,?,?,?,?,?) ");
						pstm.setInt(1, rs.getInt("ejercicio"));
						pstm.setInt(2, rs.getInt("mes"));
						pstm.setInt(3, rs.getInt("entidad"));
						pstm.setString(4, rs.getString("entidad_nombre"));
						pstm.setInt(5, rs.getInt("unidad_ejecutora"));
						pstm.setString(6, rs.getString("ue_nombre"));
						pstm.setInt(7, rs.getInt("programa"));
						pstm.setString(8, rs.getString("programa_nombre"));
						pstm.setInt(9, rs.getInt("subprograma"));
						pstm.setString(10, rs.getString("subprograma_nombre"));
						pstm.setInt(11, rs.getInt("proyecto"));
						pstm.setString(12, rs.getString("proyecto_nombre"));
						pstm.setInt(13, rs.getInt("actividad"));
						pstm.setInt(14, rs.getInt("obra"));
						pstm.setString(15, rs.getString("act_obra_nombre"));
						pstm.setInt(16, rs.getInt("renglon"));
						pstm.setString(17, rs.getString("renglon_nombre"));
						pstm.setInt(18, rs.getInt("subgrupo"));
						pstm.setString(19, rs.getString("subgrupo_nombre"));
						pstm.setInt(20, rs.getInt("grupo"));
						pstm.setString(21, rs.getString("grupo_nombre"));
						pstm.setInt(22, rs.getInt("fuente"));
						pstm.setDouble(23, rs.getDouble("ano_1"));
						pstm.setDouble(24, rs.getDouble("ano_2"));
						pstm.setDouble(25, rs.getDouble("ano_3"));
						pstm.setDouble(26, rs.getDouble("ano_4"));
						pstm.setDouble(27, rs.getDouble("ano_5"));
						pstm.setDouble(28, rs.getDouble("ano_actual"));
						pstm.setDouble(29, rs.getDouble("solicitado"));
						pstm.setDouble(30, rs.getDouble("aprobado"));
						pstm.setDouble(31, rs.getDouble("asignado"));
						pstm.setDouble(32, rs.getDouble("vigente"));
						ret = ret && pstm.executeUpdate()>0;
						rows++;
						if((rows % 10000) == 0)
							CLogger.writeConsole(String.join("Records escritos: ",String.valueOf(rows)));
						pstm.close();
					}		
				}
				
			}					
				
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 1: CEjecucionPresupuestaria.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
}
