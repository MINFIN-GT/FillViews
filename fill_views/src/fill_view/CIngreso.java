package fill_view;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import utilities.CLogger;

public class CIngreso {
	
	public static boolean loadIngresos(Connection conn, Integer ejercicio){
		boolean ret = true;
		try{
			CLogger.writeConsole("CIngresos Aprobado(Ejercicio "+ejercicio+"):");
			CLogger.writeConsole("Elminiando la data actual de MV_INGRESO");
			PreparedStatement pstm;
			pstm = conn.prepareStatement("TRUNCATE TABLE dashboard.mv_ingreso");
			pstm.executeUpdate();
			pstm.close();
			
			CLogger.writeConsole("Cargando la historia");
			pstm = conn.prepareStatement("INSERT INTO dashboard.mv_ingreso SELECT * FROM dashboard_historia.mv_ingreso WHERE ejercicio < ? ");
			pstm.setInt(1, ejercicio);
			pstm.executeUpdate();
			pstm.close();
			
			CLogger.writeConsole("Insertando valores a MV_INGRESO (Fecha Aprobado)");
			pstm = conn.prepareStatement("INSERT INTO dashboard.mv_ingreso "+
				"select ih.ejercicio, ih.entidad, e.nombre entidad_nombre,ih.unidad_ejecutora, ue.nombre unidad_ejecutora_nombre,   " + 
				"ih.fuente, ih.organismo, ih.geografico,  " + 
				"day(ih.fec_aprobado) fec_aprobado_dia, month(ih.fec_aprobado) fec_aprobado_mes, " + 
				"year(ih.fec_aprobado) fec_aprobado_anio, date_format(ih.fec_aprobado,'u') dia_semana,  " + 
				"id.recurso, r.nombre recurso_nombre, id.recurso_auxiliar, ra.nombre recurso_auxiliar_nombre, " + 
				"'Fecha Aprobado' fecha_referencia, " +
				"sum(id.monto_ingreso) monto_ingreso,  " + 
				"sum(id.saldo_ingreso) saldo_ingreso " + 
				"from sicoinprod.er_ingresos_hoja ih, sicoinprod.er_ingresos_detalle id, " + 
				"sicoinprod.cg_entidades e, sicoinprod.cg_entidades ue, " + 
				"sicoinprod.cp_recursos r, sicoinprod.cp_recursos_auxiliares ra " + 
				"where ih.ejercicio = id.ejercicio " + 
				"and ih.entidad = id.entidad " + 
				"and ih.unidad_ejecutora = id.unidad_ejecutora " + 
				"and ih.unidad_desconcentrada = id.unidad_desconcentrada " + 
				"and ih.no_cur = id.no_cur " + 
				"and e.ejercicio = ih.ejercicio " + 
				"and e.entidad = ih.entidad " + 
				"and e.unidad_ejecutora = 0 " + 
				"and ue.ejercicio = ih.ejercicio " + 
				"and ue.entidad = ih.entidad " + 
				"and ue.unidad_ejecutora = ih.unidad_ejecutora " + 
				"and r.ejercicio = ih.ejercicio " + 
				"and r.recurso = id.recurso " + 
				"and ra.ejercicio = ih.ejercicio " + 
				"and ra.recurso = id.recurso " + 
				"and ra.recurso_auxiliar = id.recurso_auxiliar " + 
				"and ih.estado = 'APROBADO' " + 
				"and ih.ejercicio = ? " + 
				"and ih.clase_registro = 'DYP' " + 
				"group by ih.ejercicio, ih.entidad, e.nombre, ih.unidad_ejecutora, ue.nombre, ih.fuente, ih.organismo, ih.geografico,   " + 
				"year(ih.fec_aprobado), month(ih.fec_aprobado), day(ih.fec_aprobado), date_format(ih.fec_aprobado,'u'), " + 
				"id.mes_ingreso, id.recurso, r.nombre, id.recurso_auxiliar, ra.nombre, 'Fecha Aprobado' ");
			pstm.setInt(1, ejercicio);
			pstm.executeUpdate();
			pstm.close();
			
			if(!conn.isClosed()){
				pstm = conn.prepareStatement("SELECT * FROM dashboard.mv_ingreso where ejercicio = ? ");
				pstm.setInt(1, ejercicio);
				ResultSet rs = pstm.executeQuery();
				boolean bconn = CMemSQL.connect();
				if(rs!=null && bconn){
					ret = true;
					int rows = 0;
					CLogger.writeConsole("CIngresos (loadIngresos mv_ingreso):");
					PreparedStatement pstm2;
					boolean first=true;
					PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_ingreso(ejercicio, fec_dia, fec_mes, fec_anio,"
							+ "entidad, entidad_nombre, unidad_ejecutora, unidad_ejecutora_nombre, fuente, organismo,"
							+ "geografico, dia_semana, recurso, recurso_nombre, recurso_auxiliar, recurso_auxiliar_nombre, monto_ingreso,"
							+ "saldo_ingreso, fecha_referencia) "
							+ "values (?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?,?,?"
							+ ") ");
					while(rs.next()){
						if(first){
							pstm2 = CMemSQL.getConnection().prepareStatement("delete from mv_ingreso where ejercicio = ?")  ;
							pstm2.setInt(1, ejercicio);
							if (pstm2.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");	
							pstm2.close();
							first=false;
						}
						
						pstm1.setInt(1, rs.getInt("ejercicio"));
						pstm1.setInt(2, rs.getInt("fec_aprobado_dia"));
						pstm1.setInt(3, rs.getInt("fec_aprobado_mes"));
						pstm1.setInt(4, rs.getInt("fec_aprobado_anio"));
						pstm1.setInt(5, rs.getInt("entidad"));
						pstm1.setString(6,rs.getString("entidad_nombre"));
						pstm1.setInt(7, rs.getInt("unidad_ejecutora"));
						pstm1.setString(8,rs.getString("unidad_ejecutora_nombre"));
						pstm1.setInt(9, rs.getInt("fuente"));
						pstm1.setInt(10, rs.getInt("organismo"));
						pstm1.setInt(11, rs.getInt("geografico"));
						pstm1.setInt(12, rs.getInt("dia_semana"));
						pstm1.setInt(13, rs.getInt("recurso"));
						pstm1.setString(14,rs.getString("recurso_nombre"));
						pstm1.setInt(15, rs.getInt("recurso_auxiliar"));
						pstm1.setString(16,rs.getString("rcurso_auxiliar_nombre"));
						pstm1.setDouble(17, rs.getDouble("monto_ingreso"));
						pstm1.setDouble(18, rs.getDouble("saldo_ingreso"));
						pstm1.setDouble(19, rs.getDouble("fecha_referencia"));
						pstm1.addBatch();
						
						rows++;
						if((rows % 10000) == 0){
							pstm1.executeBatch();
							CLogger.writeConsole(String.join("Records escritos: ",String.valueOf(rows)));
						}
					}	
					pstm1.executeBatch();
					CLogger.writeConsole(String.join("Total de records escritos: ",String.valueOf(rows)));
					rs.close();
					pstm1.close();
					pstm.close();
				}
			}
			
			
			CLogger.writeConsole("Insertando valores a MV_INGRESO (Fecha Real)");
			pstm = conn.prepareStatement("INSERT INTO dashboard.mv_ingreso "+
				"select ih.ejercicio, ih.entidad, e.nombre entidad_nombre,ih.unidad_ejecutora, ue.nombre unidad_ejecutora_nombre,   " + 
				"ih.fuente, ih.organismo, ih.geografico,  " + 
				"day(date_add(ih.fec_real,3)) fec_aprobado_dia, month(date_add(ih.fec_real,3)) fec_aprobado_mes, " + 
				"year(date_add(ih.fec_real,3)) fec_aprobado_anio, date_format(date_add(ih.fec_real,3),'u') dia_semana,  " + 
				"id.recurso, r.nombre recurso_nombre, id.recurso_auxiliar, ra.nombre recurso_auxiliar_nombre, " + 
				"'Fecha Real' fecha_referencia, " +
				"sum(id.monto_ingreso) monto_ingreso,  " + 
				"sum(id.saldo_ingreso) saldo_ingreso " + 
				"from sicoinprod.er_ingresos_hoja ih, sicoinprod.er_ingresos_detalle id, " + 
				"sicoinprod.cg_entidades e, sicoinprod.cg_entidades ue, " + 
				"sicoinprod.cp_recursos r, sicoinprod.cp_recursos_auxiliares ra " + 
				"where ih.ejercicio = id.ejercicio " + 
				"and ih.entidad = id.entidad " + 
				"and ih.unidad_ejecutora = id.unidad_ejecutora " + 
				"and ih.unidad_desconcentrada = id.unidad_desconcentrada " + 
				"and ih.no_cur = id.no_cur " + 
				"and e.ejercicio = ih.ejercicio " + 
				"and e.entidad = ih.entidad " + 
				"and e.unidad_ejecutora = 0 " + 
				"and ue.ejercicio = ih.ejercicio " + 
				"and ue.entidad = ih.entidad " + 
				"and ue.unidad_ejecutora = ih.unidad_ejecutora " + 
				"and r.ejercicio = ih.ejercicio " + 
				"and r.recurso = id.recurso " + 
				"and ra.ejercicio = ih.ejercicio " + 
				"and ra.recurso = id.recurso " + 
				"and ra.recurso_auxiliar = id.recurso_auxiliar " + 
				"and ih.estado = 'APROBADO' " + 
				"and ih.ejercicio = ? " + 
				"and ih.clase_registro = 'DYP' " + 
				"group by ih.ejercicio, ih.entidad, e.nombre, ih.unidad_ejecutora, ue.nombre, ih.fuente, ih.organismo, ih.geografico,   " + 
				"year(date_add(ih.fec_real,3)), month(date_add(ih.fec_real,3)), day(date_add(ih.fec_real,3)), date_format(date_add(ih.fec_real,3),'u'), " + 
				"id.mes_ingreso, id.recurso, r.nombre, id.recurso_auxiliar, ra.nombre, 'Fecha Real' ");
			pstm.setInt(1, ejercicio);
			pstm.executeUpdate();
			pstm.close();
			
			if(!conn.isClosed()){
				pstm = conn.prepareStatement("SELECT * FROM dashboard.mv_ingreso where ejercicio = ? ");
				pstm.setInt(1, ejercicio);
				ResultSet rs = pstm.executeQuery();
				boolean bconn = CMemSQL.connect();
				if(rs!=null && bconn){
					ret = true;
					int rows = 0;
					CLogger.writeConsole("CIngresos (loadIngresos mv_ingreso):");
					PreparedStatement pstm2;
					boolean first=true;
					PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_ingreso(ejercicio, fec_dia, fec_mes, fec_anio,"
							+ "entidad, entidad_nombre, unidad_ejecutora, unidad_ejecutora_nombre, fuente, organismo,"
							+ "geografico, dia_semana, recurso, recurso_nombre, recurso_auxiliar, recurso_auxiliar_nombre, monto_ingreso,"
							+ "saldo_ingreso, fecha_referencia) "
							+ "values (?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?,?,?"
							+ ") ");
					while(rs.next()){
						if(first){
							pstm2 = CMemSQL.getConnection().prepareStatement("delete from mv_ingreso where ejercicio = ?")  ;
							pstm2.setInt(1, ejercicio);
							if (pstm2.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");	
							pstm2.close();
							first=false;
						}
						
						pstm1.setInt(1, rs.getInt("ejercicio"));
						pstm1.setInt(2, rs.getInt("fec_aprobado_dia"));
						pstm1.setInt(3, rs.getInt("fec_aprobado_mes"));
						pstm1.setInt(4, rs.getInt("fec_aprobado_anio"));
						pstm1.setInt(5, rs.getInt("entidad"));
						pstm1.setString(6,rs.getString("entidad_nombre"));
						pstm1.setInt(7, rs.getInt("unidad_ejecutora"));
						pstm1.setString(8,rs.getString("unidad_ejecutora_nombre"));
						pstm1.setInt(9, rs.getInt("fuente"));
						pstm1.setInt(10, rs.getInt("organismo"));
						pstm1.setInt(11, rs.getInt("geografico"));
						pstm1.setInt(12, rs.getInt("dia_semana"));
						pstm1.setInt(13, rs.getInt("recurso"));
						pstm1.setString(14,rs.getString("recurso_nombre"));
						pstm1.setInt(15, rs.getInt("recurso_auxiliar"));
						pstm1.setString(16,rs.getString("rcurso_auxiliar_nombre"));
						pstm1.setDouble(17, rs.getDouble("monto_ingreso"));
						pstm1.setDouble(18, rs.getDouble("saldo_ingreso"));
						pstm1.setDouble(19, rs.getDouble("fecha_referencia"));
						pstm1.addBatch();
						
						rows++;
						if((rows % 10000) == 0){
							pstm1.executeBatch();
							CLogger.writeConsole(String.join("Records escritos: ",String.valueOf(rows)));
						}
					}	
					pstm1.executeBatch();
					CLogger.writeConsole(String.join("Total de records escritos: ",String.valueOf(rows)));
					rs.close();
					pstm1.close();
					pstm.close();
				}
			}
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 1: CIngreso.class", e);
		}
		finally{
			//CMemSQL.close();
		}
		return ret;
	}
	
	
	public static boolean loadIngresosRecursoAuxiliar(Connection conn, Integer ejercicio){
		boolean ret = true;
		try{
			CLogger.writeConsole("CIngresos");
			CLogger.writeConsole("Elminiando la data actual de MV_INGRESO_RECURSO_AUXILIAR");
			PreparedStatement pstm;
			pstm = conn.prepareStatement("TRUNCATE TABLE dashboard.mv_ingreso_recurso_auxiliar");
			pstm.executeUpdate();
			pstm.close();
			CLogger.writeConsole("Copiando historia:");
			pstm = conn.prepareStatement("INSERT INTO dashboard.mv_ingreso_recurso_auxiliar SELECT * FROM dashboard_historia.mv_ingreso_recurso_auxiliar WHERE ejercicio < ?");
			pstm.setInt(1, ejercicio);
			pstm.executeUpdate();
			pstm.close();
			
			CLogger.writeConsole("Insertando valores a MV_INGRESO_RECURSO_AUXILIAR (Fecha Aprobado)");
			pstm = conn.prepareStatement("INSERT INTO dashboard.mv_ingreso_recurso_auxiliar "+
					"SELECT recurso, recurso_nombre, recurso_auxiliar, recurso_auxiliar_nombre, ejercicio, 'Fecha Aprobado' fecha_referencia, "
					+ "sum(case when fec_mes=1 then monto_ingreso else 0 end) m1, "
					+ "sum(case when fec_mes=2 then monto_ingreso else 0 end) m2, "
					+ "sum(case when fec_mes=3 then monto_ingreso else 0 end) m3, "
					+ "sum(case when fec_mes=4 then monto_ingreso else 0 end) m4, "
					+ "sum(case when fec_mes=5 then monto_ingreso else 0 end) m5, "
					+ "sum(case when fec_mes=6 then monto_ingreso else 0 end) m6, "
					+ "sum(case when fec_mes=7 then monto_ingreso else 0 end) m7, "
					+ "sum(case when fec_mes=8 then monto_ingreso else 0 end) m8, "
					+ "sum(case when fec_mes=9 then monto_ingreso else 0 end) m9, "
					+ "sum(case when fec_mes=10 then monto_ingreso else 0 end) m10, "
					+ "sum(case when fec_mes=11 then monto_ingreso else 0 end) m11, "
					+ "sum(case when fec_mes=12 then monto_ingreso else 0 end) m12 "
					+ "from dashboard.mv_ingreso "
					+ "where ejercicio = ? "
					+ "and fecha_referencia='Fecha Aprobado'  "
					+ "group by recurso, recurso_nombre, recurso_auxiliar, recurso_auxiliar_nombre, ejercicio, 'Fecha Aprobado' ");
			pstm.setInt(1, ejercicio);
			pstm.executeUpdate();
			pstm.close();
			
			boolean bconn =  CMemSQL.connect();
			CLogger.writeConsole("Cargando datos a cache de MVP_INGRESO_RECURSO_AUXILIAR (Fecha Aprobado)");
			if(bconn){
				ret = true;
				int rows = 0;
				boolean first=true;
				PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_ingreso_recurso_auxiliar(recurso, recurso_nombre, auxiliar, auxiliar_nombre,ejercicio,"
						+ "m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12,fecha_referencia) "
						+ "values (?,?,?,?,?,"
						+ "?,?,?,?,?,?,?,?,?,?,"
						+ "?,?,?) ");
				
				pstm = conn.prepareStatement("SELECT * FROM dashboard.mv_ingreso_recurso_auxiliar where ejercicio = ?");
				pstm.setInt(1, ejercicio);
				ResultSet rs = pstm.executeQuery();
				while(rs.next()){
					if(first){
						PreparedStatement pstm2 = CMemSQL.getConnection().prepareStatement("delete from mv_ingreso_recurso_auxiliar where ejercicio =  ? and fecha_referencia='Fecha Aprobado'");
						pstm2.setInt(1, ejercicio);
						if (pstm2.executeUpdate()>0)
							CLogger.writeConsole("Registros eliminados");
						else
							CLogger.writeConsole("Sin registros para eliminar");	
						pstm2.close();
						first=false;
					}
					pstm1.setInt(1, rs.getInt("recurso"));
					pstm1.setString(2, rs.getString("recurso_nombre"));
					pstm1.setInt(3, rs.getInt("recurso_auxiliar"));
					pstm1.setString(4, rs.getString("recurso_auxiliar_nombre"));
					pstm1.setInt(5, rs.getInt("ejercicio"));
					pstm1.setDouble(6, rs.getDouble("m1"));
					pstm1.setDouble(7, rs.getDouble("m2"));
					pstm1.setDouble(8, rs.getDouble("m3"));
					pstm1.setDouble(9, rs.getDouble("m4"));
					pstm1.setDouble(10, rs.getDouble("m5"));
					pstm1.setDouble(11, rs.getDouble("m6"));
					pstm1.setDouble(12, rs.getDouble("m7"));
					pstm1.setDouble(13, rs.getDouble("m8"));
					pstm1.setDouble(14, rs.getDouble("m9"));
					pstm1.setDouble(15, rs.getDouble("m10"));
					pstm1.setDouble(16, rs.getDouble("m11"));
					pstm1.setDouble(17, rs.getDouble("m12"));
					pstm1.setString(18, "Fecha Aprobado");
					
					pstm1.addBatch();
					rows++;
					if((rows % 100) == 0)
						pstm1.executeBatch();
				}
				CLogger.writeConsole("Records escritos: "+rows);
				pstm1.executeBatch();
				pstm1.close();
				rs.close();
				pstm.close();
			}
			
			CLogger.writeConsole("Insertando valores a MV_INGRESO_RECURSO_AUXILIAR (Fecha Real)");
			pstm = conn.prepareStatement("INSERT INTO dashboard.mv_ingreso_recurso_auxiliar "+
					"SELECT recurso, recurso_nombre, recurso_auxiliar, recurso_auxiliar_nombre, ejercicio, 'Fecha Real' fecha_referencia, "
					+ "sum(case when fec_mes=1 then monto_ingreso else 0 end) m1, "
					+ "sum(case when fec_mes=2 then monto_ingreso else 0 end) m2, "
					+ "sum(case when fec_mes=3 then monto_ingreso else 0 end) m3, "
					+ "sum(case when fec_mes=4 then monto_ingreso else 0 end) m4, "
					+ "sum(case when fec_mes=5 then monto_ingreso else 0 end) m5, "
					+ "sum(case when fec_mes=6 then monto_ingreso else 0 end) m6, "
					+ "sum(case when fec_mes=7 then monto_ingreso else 0 end) m7, "
					+ "sum(case when fec_mes=8 then monto_ingreso else 0 end) m8, "
					+ "sum(case when fec_mes=9 then monto_ingreso else 0 end) m9, "
					+ "sum(case when fec_mes=10 then monto_ingreso else 0 end) m10, "
					+ "sum(case when fec_mes=11 then monto_ingreso else 0 end) m11, "
					+ "sum(case when fec_mes=12 then monto_ingreso else 0 end) m12 "
					+ "from dashboard.mv_ingreso "
					+ "where ejercicio = ? "
					+ "and fecha_referencia='Fecha Real' "
					+ "group by recurso, recurso_nombre, recurso_auxiliar, recurso_auxiliar_nombre, ejercicio, 'Fecha Real' ");
			pstm.setInt(1, ejercicio);
			pstm.executeUpdate();
			pstm.close();
			
			bconn =  CMemSQL.connect();
			CLogger.writeConsole("Cargando datos a cache de MVP_INGRESO_RECURSO_AUXILIAR (Fecha Real)");
			if(bconn){
				ret = true;
				int rows = 0;
				boolean first=true;
				PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_ingreso_recurso_auxiliar(recurso, recurso_nombre, auxiliar, auxiliar_nombre,ejercicio,"
						+ "m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12,fecha_referencia) "
						+ "values (?,?,?,?,?,"
						+ "?,?,?,?,?,?,?,?,?,?,"
						+ "?,?,?) ");
				
				pstm = conn.prepareStatement("SELECT * FROM dashboard.mv_ingreso_recurso_auxiliar where ejercicio = ?");
				pstm.setInt(1, ejercicio);
				ResultSet rs = pstm.executeQuery();
				while(rs.next()){
					if(first){
						PreparedStatement pstm2 = CMemSQL.getConnection().prepareStatement("delete from mv_ingreso_recurso_auxiliar where ejercicio =  ? and fecha_referencia='Fecha Real'");
						pstm2.setInt(1, ejercicio);
						if (pstm2.executeUpdate()>0)
							CLogger.writeConsole("Registros eliminados");
						else
							CLogger.writeConsole("Sin registros para eliminar");	
						pstm2.close();
						first=false;
					}
					pstm1.setInt(1, rs.getInt("recurso"));
					pstm1.setString(2, rs.getString("recurso_nombre"));
					pstm1.setInt(3, rs.getInt("recurso_auxiliar"));
					pstm1.setString(4, rs.getString("recurso_auxiliar_nombre"));
					pstm1.setInt(5, rs.getInt("ejercicio"));
					pstm1.setDouble(6, rs.getDouble("m1"));
					pstm1.setDouble(7, rs.getDouble("m2"));
					pstm1.setDouble(8, rs.getDouble("m3"));
					pstm1.setDouble(9, rs.getDouble("m4"));
					pstm1.setDouble(10, rs.getDouble("m5"));
					pstm1.setDouble(11, rs.getDouble("m6"));
					pstm1.setDouble(12, rs.getDouble("m7"));
					pstm1.setDouble(13, rs.getDouble("m8"));
					pstm1.setDouble(14, rs.getDouble("m9"));
					pstm1.setDouble(15, rs.getDouble("m10"));
					pstm1.setDouble(16, rs.getDouble("m11"));
					pstm1.setDouble(17, rs.getDouble("m12"));
					pstm1.setString(18, "Fecha Real");
					
					pstm1.addBatch();
					rows++;
					if((rows % 100) == 0)
						pstm1.executeBatch();
				}
				CLogger.writeConsole("Records escritos: "+rows);
				pstm1.executeBatch();
				pstm1.close();
				rs.close();
				pstm.close();
			}
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 2: CIngreso.class", e);
			ret=false;
		}
		return ret;
	}
	
	public static boolean loadIngresosRecurso(Connection conn, Integer ejercicio){
		boolean ret = true;
		try{
			CLogger.writeConsole("CIngresos");
			CLogger.writeConsole("Elminiando la data actual de MV_INGRESO_RECURSO");
			PreparedStatement pstm;
			pstm = conn.prepareStatement("TRUNCATE TABLE dashboard.mv_ingreso_recurso");
			pstm.executeUpdate();
			pstm.close();
			CLogger.writeConsole("Copiando historia:");
			pstm = conn.prepareStatement("INSERT INTO dashboard.mv_ingreso_recurso SELECT * FROM dashboard_historia.mv_ingreso_recurso WHERE ejercicio < ?");
			pstm.setInt(1, ejercicio);
			pstm.executeUpdate();
			pstm.close();
			
			CLogger.writeConsole("Insertando valores a MV_INGRESO_RECURSO (Fecha Aprobado)");
			pstm = conn.prepareStatement("INSERT INTO dashboard.mv_ingreso_recurso "+
					"SELECT recurso, recurso_nombre, ejercicio, 'Fecha Aprobado' fecha_referencia, "
					+ "sum(case when fec_mes=1 then monto_ingreso else 0 end) m1, "
					+ "sum(case when fec_mes=2 then monto_ingreso else 0 end) m2, "
					+ "sum(case when fec_mes=3 then monto_ingreso else 0 end) m3, "
					+ "sum(case when fec_mes=4 then monto_ingreso else 0 end) m4, "
					+ "sum(case when fec_mes=5 then monto_ingreso else 0 end) m5, "
					+ "sum(case when fec_mes=6 then monto_ingreso else 0 end) m6, "
					+ "sum(case when fec_mes=7 then monto_ingreso else 0 end) m7, "
					+ "sum(case when fec_mes=8 then monto_ingreso else 0 end) m8, "
					+ "sum(case when fec_mes=9 then monto_ingreso else 0 end) m9, "
					+ "sum(case when fec_mes=10 then monto_ingreso else 0 end) m10, "
					+ "sum(case when fec_mes=11 then monto_ingreso else 0 end) m11, "
					+ "sum(case when fec_mes=12 then monto_ingreso else 0 end) m12 "
					+ "from dashboard.mv_ingreso "
					+ "where ejercicio = ? "
					+ "and fecha_referencia = 'Fecha Aprobado' "
					+ "group by recurso, recurso_nombre, ejercicio, 'Fecha Aprobado' ");
			pstm.setInt(1, ejercicio);
			pstm.executeUpdate();
			pstm.close();
			
			boolean bconn =  CMemSQL.connect();
			CLogger.writeConsole("Cargando datos a cache de MVP_INGRESO_RECURSO (Fecha Aprobado)");
			if(bconn){
				ret = true;
				int rows = 0;
				boolean first=true;
				PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_ingreso_recurso(recurso, recurso_nombre, ejercicio,"
						+ "m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12, fecha_referencia) "
						+ "values (?,?,?,"
						+ "?,?,?,?,?,?,?,?,?,?,?,?,?) ");
				
				pstm = conn.prepareStatement("SELECT * FROM dashboard.mv_ingreso_recurso where ejercicio = ?");
				pstm.setInt(1, ejercicio);
				ResultSet rs = pstm.executeQuery();
				while(rs.next()){
					if(first){
						PreparedStatement pstm2 = CMemSQL.getConnection().prepareStatement("delete from mv_ingreso_recurso where ejercicio =  ? and fecha_referencia='Fecha Aprobado'");
						pstm2.setInt(1, ejercicio);
						if (pstm2.executeUpdate()>0)
							CLogger.writeConsole("Registros eliminados");
						else
							CLogger.writeConsole("Sin registros para eliminar");	
						pstm2.close();
						first=false;
					}
					pstm1.setInt(1, rs.getInt("recurso"));
					pstm1.setString(2, rs.getString("recurso_nombre"));
					pstm1.setInt(3, rs.getInt("ejercicio"));
					pstm1.setDouble(4, rs.getDouble("m1"));
					pstm1.setDouble(5, rs.getDouble("m2"));
					pstm1.setDouble(6, rs.getDouble("m3"));
					pstm1.setDouble(7, rs.getDouble("m4"));
					pstm1.setDouble(8, rs.getDouble("m5"));
					pstm1.setDouble(9, rs.getDouble("m6"));
					pstm1.setDouble(10, rs.getDouble("m7"));
					pstm1.setDouble(11, rs.getDouble("m8"));
					pstm1.setDouble(12, rs.getDouble("m9"));
					pstm1.setDouble(13, rs.getDouble("m10"));
					pstm1.setDouble(14, rs.getDouble("m11"));
					pstm1.setDouble(15, rs.getDouble("m12"));
					pstm1.setString(16, rs.getString("Fecha Aprobado"));
					
					pstm1.addBatch();
					rows++;
					if((rows % 100) == 0)
						pstm1.executeBatch();
				}
				pstm1.executeBatch();
				pstm1.close();
				rs.close();
				pstm.close();
				CLogger.writeConsole("Records escritos: "+rows);
			}
			
			CLogger.writeConsole("Insertando valores a MV_INGRESO_RECURSO (Fecha Real)");
			pstm = conn.prepareStatement("INSERT INTO dashboard.mv_ingreso_recurso "+
					"SELECT recurso, recurso_nombre, ejercicio, 'Fecha Real' fecha_referencia, "
					+ "sum(case when fec_mes=1 then monto_ingreso else 0 end) m1, "
					+ "sum(case when fec_mes=2 then monto_ingreso else 0 end) m2, "
					+ "sum(case when fec_mes=3 then monto_ingreso else 0 end) m3, "
					+ "sum(case when fec_mes=4 then monto_ingreso else 0 end) m4, "
					+ "sum(case when fec_mes=5 then monto_ingreso else 0 end) m5, "
					+ "sum(case when fec_mes=6 then monto_ingreso else 0 end) m6, "
					+ "sum(case when fec_mes=7 then monto_ingreso else 0 end) m7, "
					+ "sum(case when fec_mes=8 then monto_ingreso else 0 end) m8, "
					+ "sum(case when fec_mes=9 then monto_ingreso else 0 end) m9, "
					+ "sum(case when fec_mes=10 then monto_ingreso else 0 end) m10, "
					+ "sum(case when fec_mes=11 then monto_ingreso else 0 end) m11, "
					+ "sum(case when fec_mes=12 then monto_ingreso else 0 end) m12 "
					+ "from dashboard.mv_ingreso "
					+ "where ejercicio = ? "
					+ "and fecha_referencia = 'Fecha Real' "
					+ "group by recurso, recurso_nombre, ejercicio, 'Fecha Real' ");
			pstm.setInt(1, ejercicio);
			pstm.executeUpdate();
			pstm.close();
			
			bconn =  CMemSQL.connect();
			CLogger.writeConsole("Cargando datos a cache de MVP_INGRESO_RECURSO (Fecha Real)");
			if(bconn){
				ret = true;
				int rows = 0;
				boolean first=true;
				PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_ingreso_recurso(recurso, recurso_nombre, ejercicio,"
						+ "m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12, fecha_referencia) "
						+ "values (?,?,?,"
						+ "?,?,?,?,?,?,?,?,?,?,?,?,?) ");
				
				pstm = conn.prepareStatement("SELECT * FROM dashboard.mv_ingreso_recurso where ejercicio = ?");
				pstm.setInt(1, ejercicio);
				ResultSet rs = pstm.executeQuery();
				while(rs.next()){
					if(first){
						PreparedStatement pstm2 = CMemSQL.getConnection().prepareStatement("delete from mv_ingreso_recurso where ejercicio =  ? and fecha_referencia='Fecha Real'");
						pstm2.setInt(1, ejercicio);
						if (pstm2.executeUpdate()>0)
							CLogger.writeConsole("Registros eliminados");
						else
							CLogger.writeConsole("Sin registros para eliminar");	
						pstm2.close();
						first=false;
					}
					pstm1.setInt(1, rs.getInt("recurso"));
					pstm1.setString(2, rs.getString("recurso_nombre"));
					pstm1.setInt(3, rs.getInt("ejercicio"));
					pstm1.setDouble(4, rs.getDouble("m1"));
					pstm1.setDouble(5, rs.getDouble("m2"));
					pstm1.setDouble(6, rs.getDouble("m3"));
					pstm1.setDouble(7, rs.getDouble("m4"));
					pstm1.setDouble(8, rs.getDouble("m5"));
					pstm1.setDouble(9, rs.getDouble("m6"));
					pstm1.setDouble(10, rs.getDouble("m7"));
					pstm1.setDouble(11, rs.getDouble("m8"));
					pstm1.setDouble(12, rs.getDouble("m9"));
					pstm1.setDouble(13, rs.getDouble("m10"));
					pstm1.setDouble(14, rs.getDouble("m11"));
					pstm1.setDouble(15, rs.getDouble("m12"));
					pstm1.setString(16, rs.getString("Fecha Real"));
					
					pstm1.addBatch();
					rows++;
					if((rows % 100) == 0)
						pstm1.executeBatch();
				}
				pstm1.executeBatch();
				pstm1.close();
				rs.close();
				pstm.close();
				CLogger.writeConsole("Records escritos: "+rows);
			}
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 3: CIngreso.class", e);
		}
		return ret;
	}
	
}
