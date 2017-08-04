package fill_view;

import java.sql.Connection;
import java.sql.PreparedStatement;

import utilities.CLogger;

public class CIngreso {
	
	public static boolean loadIngresos(Connection conn, Integer ejercicio){
		boolean ret = true;
		try{
			CLogger.writeConsole("CIngresos (Ejercicio "+ejercicio+"):");
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
			
			CLogger.writeConsole("Insertando valores a MV_INGRESO");
			pstm = conn.prepareStatement("INSERT INTO dashboard.mv_ingreso "+
				"select ih.ejercicio, ih.entidad, e.nombre entidad_nombre,ih.unidad_ejecutora, ue.nombre unidad_ejecutora_nombre,   " + 
				"ih.fuente, ih.organismo, ih.geografico,  " + 
				"day(ih.fec_aprobado) fec_aprobado_dia, month(ih.fec_aprobado) fec_aprobado_mes, " + 
				"year(ih.fec_aprobado) fec_aprobado_anio, date_format(ih.fec_aprobado,'u') dia_semana,  " + 
				"id.recurso, r.nombre recurso_nombre, id.recurso_auxiliar, ra.nombre recurso_auxiliar_nombre, " + 
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
				"year(ih.fec_aprobado), month(ih.fec_aprobado), day(ih.fec_aprobado),date_format(ih.fec_aprobado,'u'), " + 
				"id.mes_ingreso, id.recurso, r.nombre, id.recurso_auxiliar, ra.nombre ");
			pstm.setInt(1, ejercicio);
			pstm.executeUpdate();
			pstm.close();
			
			if(!conn.isClosed()){
				pstm = conn.prepareStatement("SELECT * FROM dashboard.mv_ingreso where ejercicio = ? ");
				pstm.setInt(1, ejercicio);
				/*ResultSet rs = pstm.executeQuery();
				boolean bconn = CMemSQL.connect();
				if(rs!=null && bconn){
					ret = true;
					int rows = 0;
					CLogger.writeConsole("CIngresos (loadIngresos mv_ingreso):");
					PreparedStatement pstm2;
					boolean first=true;
					PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_ingreso(ejercicio, fec_aprobado_dia, fec_aprobado_mes, fec_aprobado_anio,"
							+ "entidad, entidad_nombre, unidad_ejecutora, unidad_ejecutora_nombre, fuente, organismo,"
							+ "geografico, dia_semana, recurso, recurso_nombre, recurso_auxiliar, recurso_auxiliar_nombre, monto_ingreso,"
							+ "saldo_ingreso) "
							+ "values (?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?,?) ");
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
				}*/
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
}
