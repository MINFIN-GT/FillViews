package fill_view;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;

import utilities.CLogger;

public class CEjecucionFinacieraFisica {

	public static boolean updateFinancieraFisica(Connection conn){
		boolean ret = true;
		try{
			CLogger.writeConsole("CEjecucionFinancieraFisica (Actualizacion de vista):");
			CLogger.writeConsole("Elminiando la data actual de MV_FINANCIERA_FISICA");
			PreparedStatement pstm = conn.prepareStatement("TRUNCATE TABLE dashboard.mv_financiera_fisica");
			pstm.executeUpdate();
			pstm.close();
			
			pstm = conn.prepareStatement("INSERT INTO dashboard.mv_financiera_fisica " +
					"select ep.*, ef.codigo_meta, ef.fisico_asignado, ef.fisico_modificacion, ef.fisico_ejecutado     " + 
					"			 from (     " + 
					"			 select t.ejercicio, t.mes, ep.entidad, ep.unidad_ejecutora, ep.programa, ep.subprograma, ep.proyecto, ep.actividad, ep.obra,     " + 
					"			 sum( case when ep.mes=t.mes then ep.asignado else 0 end ) financiero_asignado,     " + 
					"			 sum( case when ep.mes<=t.mes then ano_actual else 0 end) financiero_ejecutado, sum(case when ep.mes=t.mes then ep.vigente else 0 end) financiero_vigente    " + 
					"			 from dashboard.tiempo t, dashboard.mv_ejecucion_presupuestaria ep     " + 
					"			 where t.ejercicio = ep.ejercicio and t.dia = 1       " + 
					"			 group by t.ejercicio, t.mes, ep.entidad, ep.unidad_ejecutora, ep.programa, ep.subprograma, ep.proyecto, ep.actividad, ep.obra     " + 
					"			 ) ep      " + 
					"			 left outer join     " + 
					"			 (     " + 
					"			 select ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, proyecto, actividad, obra, codigo_meta, sum(fisico_asignado) fisico_asignado,   " + 
					"			 sum(fisico_modificacion) fisico_modificacion, sum(fisico_ejecutado) fisico_ejecutado    " + 
					"			 from       " + 
					"			 (     " + 
					"			 select t.ejercicio, t.mes, fis.entidad, fis.unidad_ejecutora, fis.programa, fis.subprograma, fis.proyecto, fis.actividad, fis.obra, fis.codigo_meta,    " + 
					"			 	sum( case when fis.mes <= t.mes then ejecucion else null end) fisico_ejecutado,     " + 
					"			 	avg(cantidad) fisico_asignado, sum(case when fis.mes <= t.mes then modificacion else null end) fisico_modificacion     " + 
					"			 from dashboard.mv_ejecucion_fisica fis, dashboard.tiempo t     " + 
					"			 where t.ejercicio = fis.ejercicio      " + 
					"			 and t.dia = 1      " + 
					"			 group by t.ejercicio, t.mes, fis.entidad, fis.unidad_ejecutora, fis.programa, fis.subprograma, fis.proyecto, fis.actividad, fis.obra, fis.codigo_meta      " + 
					"			 ) t1      " + 
					"			 group by ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, proyecto, actividad, obra, codigo_meta      " + 
					"			 ) ef       " + 
					"			 on (      " + 
					"			 ep.ejercicio = ef.ejercicio and ep.mes = ef.mes and ep.entidad = ef.entidad and ep.unidad_ejecutora=ef.unidad_ejecutora and ep.programa = ef.programa     " + 
					"			 and ep.subprograma = ef.subprograma and ep.proyecto = ef.proyecto and ep.actividad = ef.actividad and ep.obra = ef.obra)      " + 
					"			 where ep.ejercicio>=2014");
			pstm.executeUpdate();
			pstm.close();
			
			if(!conn.isClosed()){
				pstm = conn.prepareStatement("select *, nvl(tipo_resultado,'Otros') tipo_resultado_nombre " + 
						"from ( " + 
						"select ff.ejercicio, ff.entidad, e.entidad_nombre,  " + 
						"ff.unidad_ejecutora, e.unidad_ejecutora_nombre, " + 
						"ff.programa, e.programa_nombre, " + 
						"ff.subprograma, e.subprograma_nombre, " + 
						"ff.proyecto, e.proyecto_nombre,  " + 
						"ff.actividad, e.actividad_obra_nombre, ff.obra, ff.codigo_meta,  " + 
						"avg(case when ff.mes = 1 then ff.financiero_asignado end) financiero_asignado, " + 
						"sum(case when ff.mes = 1 then ff.financiero_ejecutado  end) financiero_ejecutado_m1, " + 
						"sum(case when ff.mes = 2 then ff.financiero_ejecutado  end) financiero_ejecutado_m2, " + 
						"sum(case when ff.mes = 3 then ff.financiero_ejecutado  end) financiero_ejecutado_m3, " + 
						"sum(case when ff.mes = 4 then ff.financiero_ejecutado  end) financiero_ejecutado_m4, " + 
						"sum(case when ff.mes = 5 then ff.financiero_ejecutado  end) financiero_ejecutado_m5, " + 
						"sum(case when ff.mes = 6 then ff.financiero_ejecutado  end) financiero_ejecutado_m6, " + 
						"sum(case when ff.mes = 7 then ff.financiero_ejecutado  end) financiero_ejecutado_m7, " + 
						"sum(case when ff.mes = 8 then ff.financiero_ejecutado  end) financiero_ejecutado_m8, " + 
						"sum(case when ff.mes = 9 then ff.financiero_ejecutado  end) financiero_ejecutado_m9, " + 
						"sum(case when ff.mes = 10 then ff.financiero_ejecutado  end) financiero_ejecutado_m10, " + 
						"sum(case when ff.mes = 11 then ff.financiero_ejecutado  end) financiero_ejecutado_m11, " + 
						"sum(case when ff.mes = 12 then ff.financiero_ejecutado  end) financiero_ejecutado_m12, " + 
						"sum(case when ff.mes = 1 then ff.financiero_vigente  end) financiero_vigente_m1,  " + 
						"sum(case when ff.mes = 2 then ff.financiero_vigente  end) financiero_vigente_m2,  " + 
						"sum(case when ff.mes = 3 then ff.financiero_vigente  end) financiero_vigente_m3,  " + 
						"sum(case when ff.mes = 4 then ff.financiero_vigente  end) financiero_vigente_m4,  " + 
						"sum(case when ff.mes = 5 then ff.financiero_vigente  end) financiero_vigente_m5,  " + 
						"sum(case when ff.mes = 6 then ff.financiero_vigente  end) financiero_vigente_m6,  " + 
						"sum(case when ff.mes = 7 then ff.financiero_vigente  end) financiero_vigente_m7,  " + 
						"sum(case when ff.mes = 8 then ff.financiero_vigente  end) financiero_vigente_m8,  " + 
						"sum(case when ff.mes = 9 then ff.financiero_vigente  end) financiero_vigente_m9,  " + 
						"sum(case when ff.mes = 10 then ff.financiero_vigente  end) financiero_vigente_m10,  " + 
						"sum(case when ff.mes = 11 then ff.financiero_vigente  end) financiero_vigente_m11,  " + 
						"sum(case when ff.mes = 12 then ff.financiero_vigente  end) financiero_vigente_m12,  " + 
						"avg(case when ff.mes = 1 then ff.fisico_asignado end) fisico_asignado, " + 
						"sum(case when ff.mes = 1 then ff.fisico_ejecutado  end) fisico_ejecutado_m1, " + 
						"sum(case when ff.mes = 2 then ff.fisico_ejecutado  end) fisico_ejecutado_m2, " + 
						"sum(case when ff.mes = 3 then ff.fisico_ejecutado  end) fisico_ejecutado_m3, " + 
						"sum(case when ff.mes = 4 then ff.fisico_ejecutado  end) fisico_ejecutado_m4, " + 
						"sum(case when ff.mes = 5 then ff.fisico_ejecutado  end) fisico_ejecutado_m5, " + 
						"sum(case when ff.mes = 6 then ff.fisico_ejecutado  end) fisico_ejecutado_m6, " + 
						"sum(case when ff.mes = 7 then ff.fisico_ejecutado  end) fisico_ejecutado_m7, " + 
						"sum(case when ff.mes = 8 then ff.fisico_ejecutado  end) fisico_ejecutado_m8, " + 
						"sum(case when ff.mes = 9 then ff.fisico_ejecutado  end) fisico_ejecutado_m9, " + 
						"sum(case when ff.mes = 10 then ff.fisico_ejecutado  end) fisico_ejecutado_m10, " + 
						"sum(case when ff.mes = 11 then ff.fisico_ejecutado  end) fisico_ejecutado_m11, " + 
						"sum(case when ff.mes = 12 then ff.fisico_ejecutado  end) fisico_ejecutado_m12, " + 
						"sum(case when ff.mes = 1 then ff.fisico_modificacion  end) fisico_modificacion_m1,  " + 
						"sum(case when ff.mes = 2 then ff.fisico_modificacion  end) fisico_modificacion_m2,  " + 
						"sum(case when ff.mes = 3 then ff.fisico_modificacion  end) fisico_modificacion_m3,  " + 
						"sum(case when ff.mes = 4 then ff.fisico_modificacion  end) fisico_modificacion_m4,  " + 
						"sum(case when ff.mes = 5 then ff.fisico_modificacion  end) fisico_modificacion_m5,  " + 
						"sum(case when ff.mes = 6 then ff.fisico_modificacion  end) fisico_modificacion_m6,  " + 
						"sum(case when ff.mes = 7 then ff.fisico_modificacion  end) fisico_modificacion_m7,  " + 
						"sum(case when ff.mes = 8 then ff.fisico_modificacion  end) fisico_modificacion_m8,  " + 
						"sum(case when ff.mes = 9 then ff.fisico_modificacion  end) fisico_modificacion_m9,  " + 
						"sum(case when ff.mes = 10 then ff.fisico_modificacion  end) fisico_modificacion_m10,  " + 
						"sum(case when ff.mes = 11 then ff.fisico_modificacion  end) fisico_modificacion_m11,  " + 
						"sum(case when ff.mes = 12 then ff.fisico_modificacion  end) fisico_modificacion_m12,  " + 
						"r.nombre_corto, r.tipo_resultado  " + 
						"from dashboard.mv_financiera_fisica ff left outer join dashboard.mv_estructura e " + 
						"on ( ff.ejercicio = e.ejercicio " + 
						"and ff.entidad = e.entidad " + 
						"and ff.unidad_ejecutora = e.unidad_ejecutora " + 
						"and ff.programa = e.programa " + 
						"and ff.subprograma = e.subprograma " + 
						"and ff.proyecto = e.proyecto " + 
						"and ff.actividad = e.actividad " + 
						"and ff.obra = e.obra " +
						") left outer join observatorio.resultados r " + 
						"on( " + 
						"	ff.ejercicio = r.ejercicio " + 
						"	and ff.entidad = r.entidad " + 
						"	and ((ff.entidad in (11130003, 11130016) and (ff.unidad_ejecutora = r.unidad_ejecutora)) OR ff.entidad not in (11130003, 11130016)) " + 
						"	and ff.programa = r.programa " + 
						"	and ff.subprograma = r.subprograma " + 
						"	and ff.entidad not in (11130018, 11130019) " + 
						") " + 
						"group by ff.ejercicio, ff.entidad, e.entidad_nombre, ff.unidad_ejecutora, e.unidad_ejecutora_nombre, ff.programa, e.programa_nombre, ff.subprograma, e.subprograma_nombre, ff.proyecto, e.proyecto_nombre, ff.actividad, e.actividad_obra_nombre, ff.obra, ff.codigo_meta, r.nombre_corto, r.tipo_resultado " + 
						") t1 " + 
						"where entidad_nombre is not null " + 
						"and ejercicio >= 2014");
				
				ResultSet rs = pstm.executeQuery();
				boolean bconn = CMemSQL.connect();
				if(rs!=null && bconn){
					ret = true;
					int rows = 0;
					CLogger.writeConsole("CEjecucionFinancieraFisica (loadEjeucionFisica mv_financiera_fisica):");
					PreparedStatement pstm2;
					boolean first=true;
					PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_financiera_fisica (ejercicio, entidad, entidad_nombre,"
							+ "unidad_ejecutora, unidad_ejecutora_nombre, programa, programa_nombre, subprograma, subprograma_nombre,"
							+ "proyecto, proyecto_nombre, actividad, actividad_obra_nombre, obra, codigo_meta, financiero_asignado, financiero_ejecutado_m1, financiero_ejecutado_m2, financiero_ejecutado_m3, financiero_ejecutado_m4, "
							+ "financiero_ejecutado_m5, financiero_ejecutado_m6, financiero_ejecutado_m7, financiero_ejecutado_m8, financiero_ejecutado_m9, financiero_ejecutado_m10, financiero_ejecutado_m11, financiero_ejecutado_m12, "
							+ "financiero_vigente_m1, financiero_vigente_m2, financiero_vigente_m3, financiero_vigente_m4, financiero_vigente_m5, financiero_vigente_m6, financiero_vigente_m7, financiero_vigente_m8, "
							+ "financiero_vigente_m9, financiero_vigente_m10, financiero_vigente_m11, financiero_vigente_m12, " 
							+ "fisico_asignado,  "
							+ "fisico_ejecutado_m1, fisico_ejecutado_m2, fisico_ejecutado_m3, fisico_ejecutado_m4, "  
							+ "fisico_ejecutado_m5, fisico_ejecutado_m6, fisico_ejecutado_m7, fisico_ejecutado_m8, fisico_ejecutado_m9, fisico_ejecutado_m10, fisico_ejecutado_m11, fisico_ejecutado_m12, " 
							+ "fisico_modificacion_m1, fisico_modificacion_m2, fisico_modificacion_m3, fisico_modificacion_m4, fisico_modificacion_m5, fisico_modificacion_m6, fisico_modificacion_m7, fisico_modificacion_m8, "  
							+ "fisico_modificacion_m9, fisico_modificacion_m10, fisico_modificacion_m11, fisico_modificacion_m12, nombre_corto, tipo_resultado)  "
							+ "values (?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?) ");
					while(rs.next()){
						if(first){
							pstm2 = CMemSQL.getConnection().prepareStatement("truncate table mv_financiera_fisica");
							pstm2.executeUpdate();
							CLogger.writeConsole("Registros eliminados");
							pstm2.close();
							first=false;
						}
						
						pstm1.setInt(1, rs.getInt("ejercicio"));
						pstm1.setInt(2, rs.getInt("entidad"));
						pstm1.setString(3,rs.getString("entidad_nombre"));
						pstm1.setInt(4, rs.getInt("unidad_ejecutora"));
						pstm1.setString(5,rs.getString("unidad_ejecutora_nombre"));
						pstm1.setInt(6, rs.getInt("programa"));
						pstm1.setString(7,rs.getString("programa_nombre"));
						pstm1.setInt(8, rs.getInt("subprograma"));
						pstm1.setString(9,rs.getString("subprograma_nombre"));
						pstm1.setInt(10, rs.getInt("proyecto"));
						pstm1.setString(11,rs.getString("proyecto_nombre"));
						pstm1.setInt(12, rs.getInt("actividad"));
						pstm1.setString(13,rs.getString("actividad_obra_nombre"));
						pstm1.setInt(14, rs.getInt("obra"));
						pstm1.setInt(15, rs.getInt("codigo_meta"));
						Double t = rs.getDouble("financiero_asignado");
						if(rs.wasNull())
							pstm1.setNull(16, Types.DOUBLE);
						else
							pstm1.setDouble(16, t);
						for(int i=1; i<13; i++){
							t = rs.getDouble("financiero_ejecutado_m"+i);
							if(rs.wasNull())
								pstm1.setNull(16+i, Types.DOUBLE);
							else
								pstm1.setDouble(16+i, t);
						}
						for(int i=1; i<13; i++){
							t = rs.getDouble("financiero_vigente_m"+i);
							if(rs.wasNull())
								pstm1.setNull(28+i, Types.DOUBLE);
							else
								pstm1.setDouble(28+i, t);
						}
						t = rs.getDouble("fisico_asignado");
						if(rs.wasNull())
							pstm1.setNull(41, Types.DOUBLE);
						else
							pstm1.setDouble(41, t);
						for(int i=1; i<13; i++){
							t = rs.getDouble("fisico_ejecutado_m"+i);
							if(rs.wasNull())
								pstm1.setNull(41+i, Types.DOUBLE);
							else
								pstm1.setDouble(41+i, t);
						}
						for(int i=1; i<13; i++){
							t = rs.getDouble("fisico_modificacion_m"+i);
							if(rs.wasNull())
								pstm1.setNull(53+i, Types.DOUBLE);
							else
								pstm1.setDouble(53+i, t);
						}
						pstm1.setString(66, rs.getString("nombre_corto"));
						pstm1.setString(67, rs.getString("tipo_resultado_nombre"));
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
			CLogger.writeFullConsole("Error 1: CEjecucionFinacieraFisica.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
	
	public static boolean updateDeuda(Connection conn){
		boolean ret = true;
		try{
			CLogger.writeConsole("CEjecucionFinancieraFisica (Actualizacion de vista):");
			CLogger.writeConsole("Elminiando la data actual de MV_DEUDA");
			PreparedStatement pstm = conn.prepareStatement("TRUNCATE TABLE dashboard.mv_deuda");
			pstm.executeUpdate();
			pstm.close();
			
			pstm = conn.prepareStatement("INSERT INTO dashboard.mv_deuda " +
					"select p.ejercicio, p.programa, e.programa_nombre, p.subprograma, e.subprograma_nombre, p.actividad, e.actividad_obra_nombre, p.renglon, p.renglon_nombre,  " + 
					"cd.tipo_deuda, cd.tipo_deuda_nombre, cd.clasificacion, cd.clasificacion_nombre, sum(case when p.mes=1 then p.asignado else 0 end) asignado, " + 
					"sum(case when p.mes = 1 then p.ano_actual else 0 end) ejecutado_m1, " + 
					"sum(case when p.mes <= 2 then p.ano_actual else 0 end) ejecutado_m2, " + 
					"sum(case when p.mes <= 3 then p.ano_actual else 0 end) ejecutado_m3, " + 
					"sum(case when p.mes <= 4 then p.ano_actual else 0 end) ejecutado_m4, " + 
					"sum(case when p.mes <= 5 then p.ano_actual else 0 end) ejecutado_m5, " + 
					"sum(case when p.mes <= 6 then p.ano_actual else 0 end) ejecutado_m6, " + 
					"sum(case when p.mes <= 7 then p.ano_actual else 0 end) ejecutado_m7, " + 
					"sum(case when p.mes <= 8 then p.ano_actual else 0 end) ejecutado_m8, " + 
					"sum(case when p.mes <= 9 then p.ano_actual else 0 end) ejecutado_m9, " + 
					"sum(case when p.mes <= 10 then p.ano_actual else 0 end) ejecutado_m10, " + 
					"sum(case when p.mes <= 11 then p.ano_actual else 0 end) ejecutado_m11, " + 
					"sum(case when p.mes <= 12 then p.ano_actual else 0 end) ejecutado_m12, " + 
					"sum(case when p.mes = 1 then p.vigente else 0 end) vigente_m1, " + 
					"sum(case when p.mes = 2 then p.vigente else 0 end) vigente_m2, " + 
					"sum(case when p.mes = 3 then p.vigente else 0 end) vigente_m3, " + 
					"sum(case when p.mes = 4 then p.vigente else 0 end) vigente_m4, " + 
					"sum(case when p.mes = 5 then p.vigente else 0 end) vigente_m5, " + 
					"sum(case when p.mes = 6 then p.vigente else 0 end) vigente_m6, " + 
					"sum(case when p.mes = 7 then p.vigente else 0 end) vigente_m7, " + 
					"sum(case when p.mes = 8 then p.vigente else 0 end) vigente_m8, " + 
					"sum(case when p.mes = 9 then p.vigente else 0 end) vigente_m9, " + 
					"sum(case when p.mes = 10 then p.vigente else 0 end) vigente_m10, " + 
					"sum(case when p.mes = 11 then p.vigente else 0 end) vigente_m11, " + 
					"sum(case when p.mes = 12 then p.vigente else 0 end) vigente_m12 " + 
					"from dashboard.mv_ejecucion_presupuestaria p, observatorio.clasificacion_deuda cd, dashboard.mv_estructura e " + 
					"where p.entidad = 11130019 " + 
					"and p.entidad = cd.entidad " + 
					"and cd.programa = p.programa " + 
					"and cd.subprograma = p.subprograma " + 
					"and cd.renglon = p.renglon " + 
					"and e.ejercicio = p.ejercicio " + 
					"and e.entidad = p.entidad " + 
					"and e.unidad_ejecutora = 0 " + 
					"and e.programa = p.programa " + 
					"and e.subprograma = p.subprograma " + 
					"and e.proyecto = 0 " + 
					"and e.actividad = p.actividad " + 
					"group by p.ejercicio, p.programa, e.programa_nombre, " + 
					" p.subprograma, e.subprograma_nombre, p.actividad, e.actividad_obra_nombre,  " + 
					" p.renglon, p.renglon_nombre, cd.tipo_deuda, cd.tipo_deuda_nombre, cd.clasificacion, cd.clasificacion_nombre");
			pstm.executeUpdate();
			pstm.close();
			
			if(!conn.isClosed()){
				pstm = conn.prepareStatement("select * " + 
						"from dashboard.mv_deuda");
				
				ResultSet rs = pstm.executeQuery();
				boolean bconn = CMemSQL.connect();
				if(rs!=null && bconn){
					ret = true;
					int rows = 0;
					CLogger.writeConsole("CEjecucionFinancieraFisica (load mv_deuda):");
					PreparedStatement pstm2;
					boolean first=true;
					PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_deuda (ejercicio, programa, programa_nombre, subprograma, subprograma_nombre,"
							+ "actividad, actividad_obra_nombre, renglon, renglon_nombre, tipo_deuda, tipo_deuda_nombre, clasificacion, clasificacion_nombre, asignado,"
							+ "ejecutado_m1, ejecutado_m2, ejecutado_m3, ejecutado_m4, ejecutado_m5, ejecutado_m6, ejecutado_m7, ejecutado_m8, ejecutado_m9, ejecutado_m10, ejecutado_m11, ejecutado_m12, "  
							+ "vigente_m1, vigente_m2, vigente_m3, vigente_m4, vigente_m5, vigente_m6, vigente_m7, vigente_m8, vigente_m9, vigente_m10, vigente_m11, vigente_m12) "
							+ "values (?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?,?) ");
					while(rs.next()){
						if(first){
							pstm2 = CMemSQL.getConnection().prepareStatement("truncate table mv_deuda");
							pstm2.executeUpdate();
							CLogger.writeConsole("Registros eliminados");
							pstm2.close();
							first=false;
						}
						
						pstm1.setInt(1, rs.getInt("ejercicio"));
						pstm1.setInt(2, rs.getInt("programa"));
						pstm1.setString(3,rs.getString("programa_nombre"));
						pstm1.setInt(4, rs.getInt("subprograma"));
						pstm1.setString(5,rs.getString("subprograma_nombre"));
						pstm1.setInt(6, rs.getInt("actividad"));
						pstm1.setString(7,rs.getString("actividad_obra_nombre"));
						pstm1.setInt(8, rs.getInt("renglon"));
						pstm1.setString(9,rs.getString("renglon_nombre"));
						pstm1.setInt(10, rs.getInt("tipo_deuda"));
						pstm1.setString(11,rs.getString("tipo_deuda_nombre"));
						pstm1.setInt(12, rs.getInt("clasificacion"));
						pstm1.setString(13,rs.getString("clasificacion_nombre"));
						pstm1.setDouble(14, rs.getDouble("asignado"));
						pstm1.setDouble(15, rs.getDouble("ejecutado_m1"));
						pstm1.setDouble(16, rs.getDouble("ejecutado_m2"));
						pstm1.setDouble(17, rs.getDouble("ejecutado_m3"));
						pstm1.setDouble(18, rs.getDouble("ejecutado_m4"));
						pstm1.setDouble(19, rs.getDouble("ejecutado_m5"));
						pstm1.setDouble(20, rs.getDouble("ejecutado_m6"));
						pstm1.setDouble(21, rs.getDouble("ejecutado_m7"));
						pstm1.setDouble(22, rs.getDouble("ejecutado_m8"));
						pstm1.setDouble(23, rs.getDouble("ejecutado_m9"));
						pstm1.setDouble(24, rs.getDouble("ejecutado_m10"));
						pstm1.setDouble(25, rs.getDouble("ejecutado_m11"));
						pstm1.setDouble(26, rs.getDouble("ejecutado_m12"));
						pstm1.setDouble(27, rs.getDouble("vigente_m1"));
						pstm1.setDouble(28, rs.getDouble("vigente_m2"));
						pstm1.setDouble(29, rs.getDouble("vigente_m3"));
						pstm1.setDouble(30, rs.getDouble("vigente_m4"));
						pstm1.setDouble(31, rs.getDouble("vigente_m5"));
						pstm1.setDouble(32, rs.getDouble("vigente_m6"));
						pstm1.setDouble(33, rs.getDouble("vigente_m7"));
						pstm1.setDouble(34, rs.getDouble("vigente_m8"));
						pstm1.setDouble(35, rs.getDouble("vigente_m9"));
						pstm1.setDouble(36, rs.getDouble("vigente_m10"));
						pstm1.setDouble(37, rs.getDouble("vigente_m11"));
						pstm1.setDouble(38, rs.getDouble("vigente_m12"));
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
			CLogger.writeFullConsole("Error 1: CEjecucionFinacieraFisica.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
	
}
