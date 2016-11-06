package fill_view;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.joda.time.DateTime;

import utilities.CLogger;

public class CEjecucionFisica {
	
	public static boolean loadEjeucionHoja(Connection conn, boolean historico, boolean des){
		DateTime date = new DateTime();
		String query = "select aprobado, clase_modificacion, cuatrimestre, descripcion, documento_gestion, ejercicio, entidad, estado, "
				+ "etapa_registro, fecha_aprobado, fecha_imputacion, fecha_real_ejec, fecha_solicitado, mes, no_cur, no_cur_original, "
				+ "no_documento, no_secuencia, solicitado, tipo_documento, unidad_ejecutora FROM sf_ejecucion_hoja_4 "+
				(!historico ? "	   WHERE ejercicio = " + date.getYear() + " " : "" );
		boolean ret = false;
		try{
			if(!conn.isClosed()){
				ResultSet rs = conn.prepareStatement(query).executeQuery();
				boolean bconn = (!des) ? CMemSQL.connect() : CMemSQL.connectdes();
				if(rs!=null && bconn){
					ret = true;
					int rows = 0;
					CLogger.writeConsole("CEjecucionFisica (loadEjeucionHoja):");
					PreparedStatement pstm;
					boolean first=true;
					SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
				    
					while(rs.next()){
						if(first){
							pstm = CMemSQL.getConnection().prepareStatement("delete from ejecucion_hoja "
									+ (!historico ? " where ejercicio=" + date.getYear() : ""))  ;
							if (pstm.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");	
							pstm.close();
							first=false;
						}
						pstm = CMemSQL.getConnection().prepareStatement("Insert INTO ejecucion_hoja(aprobado, clase_modificacion, cuatrimestre, descripcion, documento_gestion, ejercicio, entidad, estado, " + 
								"etapa_registro, fecha_aprobado, fecha_imputacion, fecha_real_ejec, fecha_solicitado, mes, no_cur, no_cur_original, " + 
								"no_documento, no_secuencia, solicitado, tipo_documento, unidad_ejecutora) "
								+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ");
						pstm.setString(1, rs.getString("aprobado"));
						pstm.setString(2, rs.getString("clase_modificacion"));
						pstm.setInt(3, rs.getInt("cuatrimestre"));
						pstm.setString(4,rs.getString("descripcion"));
						pstm.setInt(5,rs.getInt("documento_gestion"));
						pstm.setInt(6,rs.getInt("ejercicio"));
						pstm.setInt(7,rs.getInt("entidad"));
						pstm.setString(8, rs.getString("estado"));
						pstm.setString(9, rs.getString("etapa_registro"));
						Date parsedDate = rs.getString("fecha_aprobado")!=null ? dateFormat.parse(rs.getString("fecha_aprobado")) : null;
						pstm.setTimestamp(10, parsedDate!=null ? new Timestamp(parsedDate.getTime()) : null);
						parsedDate = rs.getString("fecha_imputacion")!=null ? dateFormat.parse(rs.getString("fecha_imputacion")) : null;
						pstm.setTimestamp(11, parsedDate!=null ? new Timestamp(parsedDate.getTime()) : null);
						parsedDate = rs.getString("fecha_real_ejec")!=null ? dateFormat.parse(rs.getString("fecha_real_ejec")) : null;
						pstm.setTimestamp(12, parsedDate!=null ? new Timestamp(parsedDate.getTime()) : null);
						parsedDate = rs.getString("fecha_solicitado")!=null ? dateFormat.parse(rs.getString("fecha_solicitado")) : null;
						pstm.setTimestamp(13, parsedDate!=null ? new Timestamp(parsedDate.getTime()) : null);
						pstm.setInt(14,rs.getInt("mes"));
						pstm.setInt(15,rs.getInt("no_cur"));
						pstm.setInt(16,rs.getInt("no_cur_original"));
						pstm.setString(17,rs.getString("no_documento"));
						pstm.setInt(18,rs.getInt("no_secuencia"));
						pstm.setString(19, rs.getString("solicitado"));
						pstm.setInt(20,rs.getInt("tipo_documento"));
						pstm.setInt(21,rs.getInt("unidad_ejecutora"));
						
						ret = ret && pstm.executeUpdate()>0;
						rows++;
						if((rows % 1000) == 0)
							CLogger.writeConsole(String.join("Records escritos: ",String.valueOf(rows)));
						pstm.close();
					}		
				}
			}
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 1: CEjecucionFisica.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
	
	public static boolean loadEjecucionDetalle(Connection conn, boolean historico, boolean des){
		DateTime date = new DateTime();
		String query = "select actividad, aprobado, cantidad_unidades, codigo_meta, ejercicio, entidad, fecha_aprobado, fecha_solicitado, "
				+ "geografico, nivel, nivel_meta, no_cur, obra, observaciones, programa, proyecto, solicitado, "
				+ "subprograma, unidad_ejecutora, unidad_ejecutora_origen FROM sf_ejecucion_detalle_4 "+
				(!historico ? "	   WHERE ejercicio = " + date.getYear() + " " : "" );
		boolean ret = false;
		try{
			if(!conn.isClosed()){
				ResultSet rs = conn.prepareStatement(query).executeQuery();
				boolean bconn = (!des) ? CMemSQL.connect() : CMemSQL.connectdes();
				if(rs!=null && bconn){
					ret = true;
					int rows = 0;
					SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
					CLogger.writeConsole("CEjecucionFisica (loadEjeucionDetalle):");
					PreparedStatement pstm;
					boolean first=true;
					while(rs.next()){
						if(first){
							pstm = CMemSQL.getConnection().prepareStatement("delete from ejecucion_hoja "
									+ (!historico ? " where ejercicio=" + date.getYear() : ""))  ;
							if (pstm.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");	
							pstm.close();
							first=false;
						}
						pstm = CMemSQL.getConnection().prepareStatement("Insert INTO ejecucion_detalle(actividad, aprobado, cantidad_unidades, codigo_meta, ejercicio, entidad, fecha_aprobado, fecha_solicitado," + 
								"geografico, nivel, nivel_meta, no_cur, obra, observaciones, programa, proyecto, solicitado, " + 
								"subprograma, unidad_ejecutora, unidad_ejecutora_origen) "
								+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ");
						pstm.setInt(1, rs.getInt("actividad"));
						pstm.setString(2, rs.getString("aprobado"));
						pstm.setInt(3, rs.getInt("cantidad_unidades"));
						pstm.setInt(4, rs.getInt("codigo_meta"));
						pstm.setInt(5, rs.getInt("ejercicio"));
						pstm.setInt(6, rs.getInt("entidad"));
						Date parsedDate = rs.getString("fecha_aprobado")!=null ? dateFormat.parse(rs.getString("fecha_aprobado")) : null;
						pstm.setTimestamp(7, parsedDate!=null ? new Timestamp(parsedDate.getTime()) : null);
						parsedDate = rs.getString("fecha_solicitado")!=null ? dateFormat.parse(rs.getString("fecha_solicitado")) : null;
						pstm.setTimestamp(8, parsedDate!=null ? new Timestamp(parsedDate.getTime()) : null);
						pstm.setInt(9, rs.getInt("geografico"));
						pstm.setInt(10, rs.getInt("nivel"));
						pstm.setInt(11, rs.getInt("nivel_meta"));
						pstm.setInt(12, rs.getInt("no_cur"));
						pstm.setInt(13, rs.getInt("obra"));
						pstm.setString(14, rs.getString("observaciones"));
						pstm.setInt(15, rs.getInt("programa"));
						pstm.setInt(16, rs.getInt("proyecto"));
						pstm.setString(17, rs.getString("solicitado"));
						pstm.setInt(18, rs.getInt("subprograma"));
						pstm.setInt(19, rs.getInt("unidad_ejecutora"));
						pstm.setInt(20, rs.getInt("unidad_ejecutora_origen"));
						
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
			CLogger.writeFullConsole("Error 2: CEjecucionFisica.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
	
	public static boolean loadEjeucionFisica(Connection conn){
		DateTime date = new DateTime();
		boolean ret = true;
		try{
			PreparedStatement pstm;
			pstm = conn.prepareStatement("TRUNCATE TABLE dashboard.mv_ejecucion_fisica");
			pstm.executeUpdate();
			pstm.close();
		
			CLogger.writeConsole("Insertando valores a MV_EJECUCION_FISICA");
			pstm = conn.prepareStatement("INSERT INTO dashboard.mv_ejecucion_fisica "+
				"select eh.ejercicio, eh.mes, eh.entidad, e.entidad_nombre,  " + 
				"eh.unidad_ejecutora, e.unidad_ejecutora_nombre,  " + 
				"ed.programa, e.programa_nombre, ed.subprograma, e.subprograma_nombre,  " + 
				"ed.proyecto, e.proyecto_nombre, ed.actividad, ed.obra, e.actividad_obra_nombre, " + 
				"ed.codigo_meta, m.cantidad,  " + 
				"um.nombre unidad_medida_nombre, avg(m.cantidad + m.adicion + m.disminucion) vigente, sum(ed.cantidad_unidades) ejecucion " + 
				"from sicoinprod.sf_ejecucion_hoja_4 eh, " + 
				"sicoinprod.sf_ejecucion_detalle_4 ed, " + 
				"sicoinprod.sf_meta m, " + 
				"sicoinprod.fp_unidad_medida um, dashboard.mv_estructura e " + 
				"where eh.ejercicio =  " + date.getYear() + " " +
				"and eh.ejercicio = ed.ejercicio " + 
				"and eh.entidad = ed.entidad " + 
				"and eh.unidad_ejecutora = ed.unidad_ejecutora " + 
				"and eh.no_cur = ed.no_cur " + 
				"and eh.aprobado = 'S' " + 
				"and m.ejercicio = ed.ejercicio " + 
				"and m.entidad = ed.entidad " + 
				"and m.unidad_ejecutora = ed.unidad_ejecutora " + 
				"and m.programa = ed.programa " + 
				"and m.subprograma = ed.subprograma " + 
				"and m.proyecto = ed.proyecto " + 
				"and m.actividad = ed.actividad " + 
				"and m.obra = ed.obra " + 
				"and m.codigo_meta = ed.codigo_meta " + 
				"and m.estado = 'APROBADO' " + 
				"and um.codigo = m.unidad_medida " + 
				"and um.ejercicio = m.ejercicio " + 
				"and eh.entidad = e.entidad " + 
				"and eh.unidad_ejecutora = e.unidad_ejecutora " + 
				"and ed.programa = e.programa " + 
				"and ed.subprograma = e.subprograma " + 
				"and ed.proyecto = e.proyecto " + 
				"and ed.actividad = e.actividad " + 
				"and ed.obra = e.obra " + 
				"group by eh.ejercicio, eh.mes, eh.entidad, e.entidad_nombre, eh.unidad_ejecutora, e.unidad_ejecutora_nombre, ed.programa, e.programa_nombre, ed.subprograma, e.subprograma_nombre,  " + 
				"ed.proyecto, e.proyecto_nombre, ed.actividad, ed.obra, e.actividad_obra_nombre, ed.codigo_meta, m.cantidad, um.nombre ");
			pstm.executeUpdate();
			pstm.close();
			
			if(!conn.isClosed()){
				pstm = conn.prepareStatement("SELECT * FROM dashboard.mv_ejecucion_fisica");
				ResultSet rs = pstm.executeQuery();
				boolean bconn = CMemSQL.connect();
				if(rs!=null && bconn){
					ret = true;
					int rows = 0;
					CLogger.writeConsole("CEjecucionFisica (loadEjeucionFisica):");
					PreparedStatement pstm2;
					boolean first=true;
					PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_ejecucion_fisica(ejercicio, mes, entidad, entidad_nombre,"
							+ "unidad_ejecutora, unidad_ejecutora_nombre, programa, programa_nombre, subprograma, subprograma_nombre,"
							+ "proyecto, proyecto_nombre, actividad, obra, actividad_obra_nombre, codigo_meta, cantidad,"
							+ "unidad_nombre, vigente, ejecucion) "
							+ "values (?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?,?,?,?) ");
					while(rs.next()){
						if(first){
							pstm2 = CMemSQL.getConnection().prepareStatement("delete from mv_ejecucion_fisica "
									+ " where ejercicio=" + date.getYear())  ;
							if (pstm2.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");	
							pstm2.close();
							first=false;
						}
						
						pstm1.setInt(1, rs.getInt("ejercicio"));
						pstm1.setInt(2, rs.getInt("mes"));
						pstm1.setInt(3, rs.getInt("entidad"));
						pstm1.setString(4,rs.getString("entidad_nombre"));
						pstm1.setInt(5, rs.getInt("unidad_ejecutora"));
						pstm1.setString(6,rs.getString("unidad_ejecutora_nombre"));
						pstm1.setInt(7, rs.getInt("programa"));
						pstm1.setString(8,rs.getString("programa_nombre"));
						pstm1.setInt(9, rs.getInt("subprograma"));
						pstm1.setString(10,rs.getString("subprograma_nombre"));
						pstm1.setInt(11, rs.getInt("proyecto"));
						pstm1.setString(12,rs.getString("proyecto_nombre"));
						pstm1.setInt(13, rs.getInt("actividad"));
						pstm1.setInt(14, rs.getInt("obra"));
						pstm1.setString(15,rs.getString("actividad_obra_nombre"));
						pstm1.setInt(16,rs.getInt("codigo_meta"));
						pstm1.setDouble(17, rs.getDouble("cantidad"));
						pstm1.setString(18, rs.getString("unidad_medida_nombre"));
						pstm1.setDouble(19, rs.getDouble("vigente"));
						pstm1.setDouble(20, rs.getDouble("ejecucion"));
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
			CLogger.writeFullConsole("Error 3: CEjecucionFisica.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
}
