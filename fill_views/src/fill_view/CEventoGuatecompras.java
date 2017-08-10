package fill_view;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import utilities.CLogger;

public class CEventoGuatecompras {

public static boolean loadEventosGC(Connection conn, Integer ejercicio){
		
		boolean ret = false;
		try{
			if( !conn.isClosed() && CMemSQL.connect()){
				ret = true;

				CLogger.writeConsole("CEventoGuatecompras (Ejercicio "+ejercicio+"):");
				CLogger.writeConsole("Eliminando data actual:");
				PreparedStatement pstm = conn.prepareStatement("TRUNCATE TABLE dashboard.mv_evento_gc");
				pstm.executeUpdate();
				pstm.close();
				
				CLogger.writeConsole("Copiando historia:");
				pstm = conn.prepareStatement("INSERT INTO dashboard.mv_evento_gc SELECT * FROM dashboard_historia.mv_evento_gc WHERE ano_publicacion < ?");
				pstm.setInt(1, ejercicio);
				pstm.executeUpdate();
				pstm.close();
				
				
				CLogger.writeConsole("Insertando valores a MV_EVENTO_GC");
				pstm = conn.prepareStatement("insert into table dashboard.mv_evento_gc "+
						"SELECT tep.TIPO_ENTIDAD						 tipo_entidad_padre, " + 
						"       tep.nombre                             tipo_entidad_padre_nombre, " + 
						"	   te.TIPO_ENTIDAD						 tipo_entidad, " + 
						"	   te.nombre                              tipo_entidad_nombre, " + 
						"	   ec.ENTIDAD_COMPRADORA					 entidad_compradora, " + 
						"       ec.nombre                              entidad_compradora_nombre, " + 
						"	   uc.UNIDAD_COMPRADORA					 unidad_compradora, " + 
						"       uc.nombre                              unidad_compradora_nombre, " + 
						"       c.nog_concurso, " + 
						"       c.descripcion, " + 
						"       c.fecha_publicacion, " + 
						"       month(c.fecha_publicacion) mes_publicacion, " + 
						"       year(c.fecha_publicacion)  ano_publicacion, " + 
						"       m.MODALIDAD							 modalidad, " + 
						"	   m.nombre                               modalidad_nombre, " + 
						"	   esc.ESTATUS_CONCURSO					 estatus_concurso, " + 
						"       esc.nombre                             estatus_concurso_nombre, " + 
						"       a.fecha_adjudicacion, " +
						"		sum(a.monto)						 monto "+
						"  FROM guatecompras.gc_entidad                 ec, " + 
						"       guatecompras.gc_entidad                 uc, " + 
						"       guatecompras.gc_concurso                c  " + 
						"       left outer join guatecompras.gc_adjudicacion a on (a.nog_concurso=c.nog_concurso and a.estatus = 0), " + 
						"       guatecompras.gc_tipo_entidad            te, " + 
						"       guatecompras.gc_modalidad               m, " + 
						"       guatecompras.gc_tipo_entidad            tep, " + 
						"       guatecompras.gc_estatus_concurso        esc " + 
						" WHERE     ec.entidad_compradora = c.entidad_compradora " + 
						"       AND ec.unidad_compradora = 0 " + 
						"       AND uc.entidad_compradora = c.entidad_compradora " + 
						"       AND uc.unidad_compradora = c.unidad_compradora " + 
						"       AND te.tipo_entidad = ec.tipo_entidad " + 
						"       AND m.modalidad = c.modalidad " + 
						"       AND tep.tipo_entidad = te.tipo_entidad_padre " + 
						"       AND esc.estatus_concurso = c.estatus_concurso " + 
						"		AND a.estatus = 1 " +	
						"		AND YEAR(c.fecha_publicacion) = ? "+
						" GROUP BY tep.TIPO_ENTIDAD, tep.nombre, te.TIPO_ENTIDAD, te.nombre, ec.ENTIDAD_COMPRADORA, ec.nombre, uc.UNIDAD_COMPRADORA, uc.nombre, c.nog_concurso, c.descripcion, c.fecha_publicacion, month(c.fecha_publicacion), year(c.fecha_publicacion), m.MODALIDAD, m.nombre, esc.ESTATUS_CONCURSO, esc.nombre, a.fecha_adjudicacion");
				pstm.setInt(1, ejercicio);
				pstm.executeUpdate();
				pstm.close();
				
				boolean bconn =  CMemSQL.connect();
				CLogger.writeConsole("Cargando datos a cache de MV_EVENTO_GC");
				if(bconn){
					ret = true;
					int rows = 0;
					boolean first=true;
					PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_evento_gc(tipo_entidad_padre, tipo_entidad_padre_nombre, "
							+ "tipo_entidad, tipo_entidad_nombre, entidad_compradora, entidad_compradora_nombre, unidad_compradora, unidad_compradora_nombre, " + 
							"nog_concurso, descripcion, fecha_publicacion, mes_publicacion, ano_publicacion, modalidad, modalidad_nombre, estatus_concurso, estatus_concurso_nombre," + 
							"monto, fecha_adjudicacion) "
							+ "values (?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?,?,?) ");
					
					pstm = conn.prepareStatement("SELECT * FROM dashboard.mv_evento_gc where ano_publicacion = ?");
						pstm.setInt(1, ejercicio);
						pstm.setFetchSize(10000);
						ResultSet rs = pstm.executeQuery();
						DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
						String fecha = null;
						while(rs!=null && rs.next()){
							if(first){
								PreparedStatement pstm2 = CMemSQL.getConnection().prepareStatement("delete from mv_evento_gc where ano_publicacion =  ? ");
								pstm2.setInt(1, ejercicio);
								if (pstm2.executeUpdate()>0)
									CLogger.writeConsole("Registros eliminados");
								else
									CLogger.writeConsole("Sin registros para eliminar");	
								pstm2.close();
								first=false;
							}
							pstm1.setInt(1, rs.getInt("tipo_entidad_padre"));
							pstm1.setString(2, rs.getString("tipo_entidad_padre_nombre"));
							pstm1.setInt(3, rs.getInt("tipo_entidad"));
							pstm1.setString(4, rs.getString("tipo_entidad_nombre"));
							pstm1.setInt(5, rs.getInt("entidad_compradora"));
							pstm1.setString(6, rs.getString("entidad_compradora_nombre"));
							pstm1.setInt(7, rs.getInt("unidad_compradora"));
							pstm1.setString(8, rs.getString("unidad_compradora_nombre"));
							pstm1.setInt(9, rs.getInt("nog_concurso"));
							pstm1.setString(10, rs.getString("descripcion"));
							fecha = rs.getString("fecha_publicacion");
							if(fecha!=null)
								pstm1.setDate(11, new java.sql.Date(df.parse(fecha).getTime()));
							else
								pstm1.setNull(11, java.sql.Types.DATE);
							pstm1.setInt(12, rs.getInt("mes_publicacion"));
							pstm1.setInt(13, rs.getInt("ano_publicacion"));
							pstm1.setInt(14, rs.getInt("modalidad"));
							pstm1.setString(15, rs.getString("modalidad_nombre"));
							pstm1.setInt(16, rs.getInt("estatus_concurso"));
							pstm1.setString(17, rs.getString("estatus_concurso_nombre"));
							pstm1.setDouble(18, rs.getDouble("monto"));
							fecha = rs.getString("fecha_adjudicacion");
							if(fecha!=null)
								pstm1.setDate(19, new java.sql.Date(df.parse(fecha).getTime()));
							else
								pstm1.setNull(19, java.sql.Types.DATE);
							pstm1.addBatch();
							rows++;
							
							if((rows % 10000) == 0)
								pstm1.executeBatch();
						}
						pstm1.executeBatch();
						rs.close();
						pstm.close();
					
					CLogger.writeConsole("Records escritos Totales: "+rows);
					pstm1.close();
					
					
					CLogger.writeConsole("Eliminando data actual de MV_EVENTO_GC_GROUP:");
					
					pstm = conn.prepareStatement("TRUNCATE TABLE dashboard.mv_evento_gc_group");
					pstm.executeUpdate();
					pstm.close();
					
					CLogger.writeConsole("Copiando historia:");
					pstm = conn.prepareStatement("INSERT INTO dashboard.mv_evento_gc_group SELECT * FROM dashboard_historia.mv_evento_gc_group WHERE ano_publicacion < ?");
					pstm.setInt(1, ejercicio);
					pstm.executeUpdate();
					pstm.close();
					
					CLogger.writeConsole("Insertando valores a MV_EVENTOS_GC_GROUP");
					pstm = conn.prepareStatement("insert into table dashboard_historia.mv_evento_gc_group "+
							"select "+ejercicio+" ano_publicacion, entidad_compradora, entidad_compradora_nombre, " + 
							"sum(case when mes_publicacion=1 and ano_publicacion=year(current_timestamp)-2 then 1 else 0 end) mes_1_ano_1, " + 
							"sum(case when mes_publicacion=1 and ano_publicacion=year(current_timestamp)-1 then 1 else 0 end) mes_1_ano_2, " + 
							"sum(case when mes_publicacion=1 and ano_publicacion=year(current_timestamp) then 1 else 0 end) mes_1_ano_actual, " + 
							"sum(case when mes_publicacion=2 and ano_publicacion=year(current_timestamp)-2 then 1 else 0 end) mes_2_ano_1, " + 
							"sum(case when mes_publicacion=2 and ano_publicacion=year(current_timestamp)-1 then 1 else 0 end) mes_2_ano_2, " + 
							"sum(case when mes_publicacion=2 and ano_publicacion=year(current_timestamp) then 1 else 0 end) mes_2_ano_actual, " + 
							"sum(case when mes_publicacion=3 and ano_publicacion=year(current_timestamp)-2 then 1 else 0 end) mes_3_ano_1, " + 
							"sum(case when mes_publicacion=3 and ano_publicacion=year(current_timestamp)-1 then 1 else 0 end) mes_3_ano_2, " + 
							"sum(case when mes_publicacion=3 and ano_publicacion=year(current_timestamp) then 1 else 0 end) mes_3_ano_actual, " + 
							"sum(case when mes_publicacion=4 and ano_publicacion=year(current_timestamp)-2 then 1 else 0 end) mes_4_ano_1, " + 
							"sum(case when mes_publicacion=4 and ano_publicacion=year(current_timestamp)-1 then 1 else 0 end) mes_4_ano_2, " + 
							"sum(case when mes_publicacion=4 and ano_publicacion=year(current_timestamp) then 1 else 0 end) mes_4_ano_actual, " + 
							"sum(case when mes_publicacion=5 and ano_publicacion=year(current_timestamp)-2 then 1 else 0 end) mes_5_ano_1, " + 
							"sum(case when mes_publicacion=5 and ano_publicacion=year(current_timestamp)-1 then 1 else 0 end) mes_5_ano_2, " + 
							"sum(case when mes_publicacion=5 and ano_publicacion=year(current_timestamp) then 1 else 0 end) mes_5_ano_actual, " + 
							"sum(case when mes_publicacion=6 and ano_publicacion=year(current_timestamp)-2 then 1 else 0 end) mes_6_ano_1, " + 
							"sum(case when mes_publicacion=6 and ano_publicacion=year(current_timestamp)-1 then 1 else 0 end) mes_6_ano_2, " + 
							"sum(case when mes_publicacion=6 and ano_publicacion=year(current_timestamp) then 1 else 0 end) mes_6_ano_actual, " + 
							"sum(case when mes_publicacion=7 and ano_publicacion=year(current_timestamp)-2 then 1 else 0 end) mes_7_ano_1, " + 
							"sum(case when mes_publicacion=7 and ano_publicacion=year(current_timestamp)-1 then 1 else 0 end) mes_7_ano_2, " + 
							"sum(case when mes_publicacion=7 and ano_publicacion=year(current_timestamp) then 1 else 0 end) mes_7_ano_actual, " + 
							"sum(case when mes_publicacion=8 and ano_publicacion=year(current_timestamp)-2 then 1 else 0 end) mes_8_ano_1, " + 
							"sum(case when mes_publicacion=8 and ano_publicacion=year(current_timestamp)-1 then 1 else 0 end) mes_8_ano_2, " + 
							"sum(case when mes_publicacion=8 and ano_publicacion=year(current_timestamp) then 1 else 0 end) mes_8_ano_actual, " + 
							"sum(case when mes_publicacion=9 and ano_publicacion=year(current_timestamp)-2 then 1 else 0 end) mes_9_ano_1, " + 
							"sum(case when mes_publicacion=9 and ano_publicacion=year(current_timestamp)-1 then 1 else 0 end) mes_9_ano_2, " + 
							"sum(case when mes_publicacion=9 and ano_publicacion=year(current_timestamp) then 1 else 0 end) mes_9_ano_actual, " + 
							"sum(case when mes_publicacion=10 and ano_publicacion=year(current_timestamp)-2 then 1 else 0 end) mes_10_ano_1, " + 
							"sum(case when mes_publicacion=10 and ano_publicacion=year(current_timestamp)-1 then 1 else 0 end) mes_10_ano_2, " + 
							"sum(case when mes_publicacion=10 and ano_publicacion=year(current_timestamp) then 1 else 0 end) mes_10_ano_actual, " + 
							"sum(case when mes_publicacion=11 and ano_publicacion=year(current_timestamp)-2 then 1 else 0 end) mes_11_ano_1, " + 
							"sum(case when mes_publicacion=11 and ano_publicacion=year(current_timestamp)-1 then 1 else 0 end) mes_11_ano_2, " + 
							"sum(case when mes_publicacion=11 and ano_publicacion=year(current_timestamp) then 1 else 0 end) mes_11_ano_actual, " + 
							"sum(case when mes_publicacion=12 and ano_publicacion=year(current_timestamp)-2 then 1 else 0 end) mes_12_ano_1, " + 
							"sum(case when mes_publicacion=12 and ano_publicacion=year(current_timestamp)-1 then 1 else 0 end) mes_12_ano_2, " + 
							"sum(case when mes_publicacion=12 and ano_publicacion=year(current_timestamp) then 1 else 0 end) mes_12_ano_actual " + 
							"from ( " + 
							"select ano_publicacion, mes_publicacion, entidad_compradora, entidad_compradora_nombre, nog_concurso, sum(monto) " + 
							"from dashboard_historia.mv_evento_gc " + 
							"where tipo_entidad = 4 " + 
							"and ano_publicacion between "+(ejercicio-2)+" and "+(ejercicio)+" "+ 
							"group by ano_publicacion, mes_publicacion, entidad_compradora, entidad_compradora_nombre, nog_concurso " + 
							") t1 " + 
							"group by entidad_compradora, entidad_compradora_nombre");
					pstm.executeUpdate();
					pstm.close();
					
					ret = true;
					PreparedStatement pstm2 = CMemSQL.getConnection().prepareStatement("delete from mv_evento_gc_group where ano_publicacion =  ? ");
					pstm2.setInt(1, ejercicio);
					if (pstm2.executeUpdate()>0)
						CLogger.writeConsole("Registros eliminados");
					else
						CLogger.writeConsole("Sin registros para eliminar");	
					pstm2.close();
					
					pstm1 = CMemSQL.getConnection().prepareStatement("insert into minfin.mv_evento_gc_group "+
							"select "+ejercicio+" ano_publicacion, entidad_compradora, entidad_compradora_nombre, " + 
							"sum(case when mes_publicacion=1 and ano_publicacion="+(ejercicio-2)+" then 1 else 0 end) mes_1_ano_1, " + 
							"sum(case when mes_publicacion=1 and ano_publicacion="+(ejercicio-1)+" then 1 else 0 end) mes_1_ano_2, " + 
							"sum(case when mes_publicacion=1 and ano_publicacion="+ejercicio+" then 1 else 0 end) mes_1_ano_actual, " + 
							"sum(case when mes_publicacion=2 and ano_publicacion="+(ejercicio-2)+" then 1 else 0 end) mes_2_ano_1, " + 
							"sum(case when mes_publicacion=2 and ano_publicacion="+(ejercicio-1)+" then 1 else 0 end) mes_2_ano_2, " + 
							"sum(case when mes_publicacion=2 and ano_publicacion="+ejercicio+" then 1 else 0 end) mes_2_ano_actual, " + 
							"sum(case when mes_publicacion=3 and ano_publicacion="+(ejercicio-2)+" then 1 else 0 end) mes_3_ano_1, " + 
							"sum(case when mes_publicacion=3 and ano_publicacion="+(ejercicio-1)+" then 1 else 0 end) mes_3_ano_2, " + 
							"sum(case when mes_publicacion=3 and ano_publicacion="+ejercicio+" then 1 else 0 end) mes_3_ano_actual, " + 
							"sum(case when mes_publicacion=4 and ano_publicacion="+(ejercicio-2)+" then 1 else 0 end) mes_4_ano_1, " + 
							"sum(case when mes_publicacion=4 and ano_publicacion="+(ejercicio-1)+" then 1 else 0 end) mes_4_ano_2, " + 
							"sum(case when mes_publicacion=4 and ano_publicacion="+ejercicio+" then 1 else 0 end) mes_4_ano_actual, " + 
							"sum(case when mes_publicacion=5 and ano_publicacion="+(ejercicio-2)+" then 1 else 0 end) mes_5_ano_1, " + 
							"sum(case when mes_publicacion=5 and ano_publicacion="+(ejercicio-1)+" then 1 else 0 end) mes_5_ano_2, " + 
							"sum(case when mes_publicacion=5 and ano_publicacion="+ejercicio+" then 1 else 0 end) mes_5_ano_actual, " + 
							"sum(case when mes_publicacion=6 and ano_publicacion="+(ejercicio-2)+" then 1 else 0 end) mes_6_ano_1, " + 
							"sum(case when mes_publicacion=6 and ano_publicacion="+(ejercicio-1)+" then 1 else 0 end) mes_6_ano_2, " + 
							"sum(case when mes_publicacion=6 and ano_publicacion="+ejercicio+" then 1 else 0 end) mes_6_ano_actual, " + 
							"sum(case when mes_publicacion=7 and ano_publicacion="+(ejercicio-2)+" then 1 else 0 end) mes_7_ano_1, " + 
							"sum(case when mes_publicacion=7 and ano_publicacion="+(ejercicio-1)+" then 1 else 0 end) mes_7_ano_2, " + 
							"sum(case when mes_publicacion=7 and ano_publicacion="+ejercicio+" then 1 else 0 end) mes_7_ano_actual, " + 
							"sum(case when mes_publicacion=8 and ano_publicacion="+(ejercicio-2)+" then 1 else 0 end) mes_8_ano_1, " + 
							"sum(case when mes_publicacion=8 and ano_publicacion="+(ejercicio-1)+" then 1 else 0 end) mes_8_ano_2, " + 
							"sum(case when mes_publicacion=8 and ano_publicacion="+ejercicio+" then 1 else 0 end) mes_8_ano_actual, " + 
							"sum(case when mes_publicacion=9 and ano_publicacion="+(ejercicio-2)+" then 1 else 0 end) mes_9_ano_1, " + 
							"sum(case when mes_publicacion=9 and ano_publicacion="+(ejercicio-1)+" then 1 else 0 end) mes_9_ano_2, " + 
							"sum(case when mes_publicacion=9 and ano_publicacion="+ejercicio+" then 1 else 0 end) mes_9_ano_actual, " + 
							"sum(case when mes_publicacion=10 and ano_publicacion="+(ejercicio-2)+" then 1 else 0 end) mes_10_ano_1, " + 
							"sum(case when mes_publicacion=10 and ano_publicacion="+(ejercicio-1)+" then 1 else 0 end) mes_10_ano_2, " + 
							"sum(case when mes_publicacion=10 and ano_publicacion="+ejercicio+" then 1 else 0 end) mes_10_ano_actual, " + 
							"sum(case when mes_publicacion=11 and ano_publicacion="+(ejercicio-2)+" then 1 else 0 end) mes_11_ano_1, " + 
							"sum(case when mes_publicacion=11 and ano_publicacion="+(ejercicio-1)+" then 1 else 0 end) mes_11_ano_2, " + 
							"sum(case when mes_publicacion=11 and ano_publicacion="+ejercicio+" then 1 else 0 end) mes_11_ano_actual, " + 
							"sum(case when mes_publicacion=12 and ano_publicacion="+(ejercicio-2)+" then 1 else 0 end) mes_12_ano_1, " + 
							"sum(case when mes_publicacion=12 and ano_publicacion="+(ejercicio-1)+" then 1 else 0 end) mes_12_ano_2, " + 
							"sum(case when mes_publicacion=12 and ano_publicacion="+ejercicio+" then 1 else 0 end) mes_12_ano_actual " +  
							"from ( " + 
							"select ano_publicacion, mes_publicacion, entidad_compradora, entidad_compradora_nombre, nog_concurso, sum(monto) " + 
							"from minfin.mv_evento_gc " + 
							"where tipo_entidad = 4 " + 
							"and ano_publicacion between "+(ejercicio-2)+" and "+(ejercicio)+" "+ 
							"group by ano_publicacion, mes_publicacion, entidad_compradora, entidad_compradora_nombre, nog_concurso " + 
							") t1 " + 
							"group by entidad_compradora, entidad_compradora_nombre");
					rows = pstm1.executeUpdate();
					
					CLogger.writeConsole("Records escritos Totales: "+rows);
					pstm1.close();
				}
				
			}					
				
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 1: CEventoGuatecompras.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}

	public static boolean loadEventosGCHistoria(Connection conn, Integer ejercicio){
		
		boolean ret = false;
		try{
			if( !conn.isClosed() && CMemSQL.connect()){
				ret = true;
	
				CLogger.writeConsole("CEventosGuatecompras Historia (Ejercicio "+ejercicio+"):");
				CLogger.writeConsole("Eliminando data actual:");
				PreparedStatement pstm;
				
				pstm = conn.prepareStatement("DROP TABLE IF EXISTS dashboard_historia.mv_eventos_gc_temp PURGE");
				pstm.executeUpdate();
				pstm.close();
				pstm = conn.prepareStatement("CREATE TABLE dashboard_historia.mv_eventos_gc_temp AS SELECT * FROM dashboard_historia.mv_eventos_gc WHERE YEAR(fecha_publicacion)<>?");
				pstm.setInt(1, ejercicio);
				pstm.executeUpdate();
				pstm.close();
				pstm = conn.prepareStatement("TRUNCATE TABLE dashboard_historia.mv_eventos_gc");
				pstm.executeUpdate();
				pstm.close();
				pstm = conn.prepareStatement("INSERT INTO dashboard_historia.mv_eventos_gc SELECT * FROM dashboard_historia.mv_eventos_gc_temp");
				pstm.executeUpdate();
				pstm.close();
				pstm = conn.prepareStatement("DROP TABLE IF EXISTS dashboard_historia.mv_eventos_gc_temp PURGE");
				pstm.executeUpdate();
				pstm.close();
				
				
				
				CLogger.writeConsole("Insertando valores a MV_EVENTOS_GC");
				pstm = conn.prepareStatement("insert into table dashboard_historia.mv_eventos_gc "+
						"SELECT tep.TIPO_ENTIDAD						 tipo_entidad_padre, " + 
						"       tep.nombre                             tipo_entidad_padre_nombre, " + 
						"	   te.TIPO_ENTIDAD						 tipo_entidad, " + 
						"	   te.nombre                              tipo_entidad_nombre, " + 
						"	   ec.ENTIDAD_COMPRADORA					 entidad_compradora, " + 
						"       ec.nombre                              entidad_compradora_nombre, " + 
						"	   uc.UNIDAD_COMPRADORA					 unidad_compradora, " + 
						"       uc.nombre                              unidad_compradora_nombre, " + 
						"       c.nog_concurso, " + 
						"       c.descripcion, " + 
						"       c.fecha_publicacion, " + 
						"       month(c.fecha_publicacion) mes_publicacion, " + 
						"       year(c.fecha_publicacion)  ano_publicacion, " + 
						"       m.MODALIDAD							 modalidad, " + 
						"	   m.nombre                               modalidad_nombre, " + 
						"	   esc.ESTATUS_CONCURSO					 estatus_concurso, " + 
						"       esc.nombre                             estatus_concurso_nombre, " + 
						"       a.fecha_adjudicacion, " +
						"		sum(a.monto)						 monto "+ 
						"  FROM guatecompras.gc_entidad                 ec, " + 
						"       guatecompras.gc_entidad                 uc, " + 
						"       guatecompras.gc_concurso                c  " + 
						"       left outer join guatecompras.gc_adjudicacion a on (a.nog_concurso=c.nog_concurso and a.estatus = 0), " + 
						"       guatecompras.gc_tipo_entidad            te, " + 
						"       guatecompras.gc_modalidad               m, " + 
						"       guatecompras.gc_tipo_entidad            tep, " + 
						"       guatecompras.gc_estatus_concurso        esc " + 
						" WHERE     ec.entidad_compradora = c.entidad_compradora " + 
						"       AND ec.unidad_compradora = 0 " + 
						"       AND uc.entidad_compradora = c.entidad_compradora " + 
						"       AND uc.unidad_compradora = c.unidad_compradora " + 
						"       AND te.tipo_entidad = ec.tipo_entidad " + 
						"       AND m.modalidad = c.modalidad " + 
						"       AND tep.tipo_entidad = te.tipo_entidad_padre " + 
						"       AND esc.estatus_concurso = c.estatus_concurso " + 
						"		AND a.estatus = 1 " +
						"		AND YEAR(c.fecha_publicacion) = ? " +
						" GROUP BY tep.TIPO_ENTIDAD, tep.nombre, te.TIPO_ENTIDAD, te.nombre, ec.ENTIDAD_COMPRADORA, ec.nombre, uc.UNIDAD_COMPRADORA, uc.nombre, c.nog_concurso, c.descripcion, c.fecha_publicacion, month(c.fecha_publicacion), year(c.fecha_publicacion), m.MODALIDAD, m.nombre, esc.ESTATUS_CONCURSO, esc.nombre, a.fecha_adjudicacion");
				pstm.setInt(1, ejercicio);
				pstm.executeUpdate();
				pstm.close();
				
				
				boolean bconn =  CMemSQL.connect();
				CLogger.writeConsole("Cargando datos a cache de MV_EVENTO_GC");
				if(bconn){
					ret = true;
					int rows = 0;
					boolean first=true;
					PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_evento_gc(tipo_entidad_padre, tipo_entidad_padre_nombre, "
							+ "tipo_entidad, tipo_entidad_nombre, entidad_compradora, entidad_compradora_nombre, unidad_compradora, unidad_compradora_nombre, " + 
							"nog_concurso, descripcion, fecha_publicacion, mes_publicacion, ano_publicacion, modalidad, modalidad_nombre, estatus_concurso, estatus_concurso_nombre," + 
							"monto, fecha_adjudicacion) "
							+ "values (?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?,?,?) ");
					
					pstm = conn.prepareStatement("SELECT * FROM dashboard_historia.mv_evento_gc where ejercicio = ?");
						pstm.setInt(1, ejercicio);
						pstm.setFetchSize(10000);
						ResultSet rs = pstm.executeQuery();
						DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
						String fecha = null;
						while(rs!=null && rs.next()){
							if(first){
								PreparedStatement pstm2 = CMemSQL.getConnection().prepareStatement("delete from mv_evento_gc where YEAR(fecha_publicacion) =  ? ");
								pstm2.setInt(1, ejercicio);
								if (pstm2.executeUpdate()>0)
									CLogger.writeConsole("Registros eliminados");
								else
									CLogger.writeConsole("Sin registros para eliminar");	
								pstm2.close();
								first=false;
							}
							pstm1.setInt(1, rs.getInt("tipo_entidad_padre"));
							pstm1.setString(2, rs.getString("tipo_entidad_padre_nombre"));
							pstm1.setInt(3, rs.getInt("tipo_entidad"));
							pstm1.setString(4, rs.getString("tipo_entidad_nombre"));
							pstm1.setInt(5, rs.getInt("entidad_compradora"));
							pstm1.setString(6, rs.getString("entidad_compradora_nombre"));
							pstm1.setInt(7, rs.getInt("unidad_compradora"));
							pstm1.setString(8, rs.getString("unidad_compradora_nombre"));
							pstm1.setInt(9, rs.getInt("nog_concurso"));
							pstm1.setString(10, rs.getString("descripcion"));
							fecha = rs.getString("fecha_publicacion");
							if(fecha!=null)
								pstm1.setDate(11, new java.sql.Date( df.parse(fecha).getTime()));
							else
								pstm1.setDate(11, null);
							pstm1.setInt(12, rs.getInt("mes_publicacion"));
							pstm1.setInt(13, rs.getInt("ano_publicacion"));
							pstm1.setInt(14, rs.getInt("modalidad"));
							pstm1.setString(15, rs.getString("modalidad_nombre"));
							pstm1.setInt(16, rs.getInt("estatus_concurso"));
							pstm1.setString(17, rs.getString("estatus_concurso_nombre"));
							pstm1.setDouble(18, rs.getDouble("monto"));
							fecha = rs.getString("fecha_adjudicacion");
							if(fecha!=null)
								pstm1.setDate(19, new java.sql.Date( df.parse(fecha).getTime()));
							else
								pstm1.setDate(19, null);
							
							pstm1.addBatch();
							rows++;
							
							if((rows % 10000) == 0)
								pstm1.executeBatch();
						}
						pstm1.executeBatch();
						rs.close();
						pstm.close();
					
					CLogger.writeConsole("Records escritos Totales: "+rows);
					pstm1.close();
					
					CLogger.writeConsole("Eliminando data actual de MV_EVENTO_GC_GROUP:");
					
					pstm = conn.prepareStatement("DROP TABLE IF EXISTS dashboard_historia.mv_eventos_gc_group_temp PURGE");
					pstm.executeUpdate();
					pstm.close();
					pstm = conn.prepareStatement("CREATE TABLE dashboard_historia.mv_eventos_gc_group_temp AS SELECT * FROM dashboard_historia.mv_eventos_gc_group WHERE ano_publicacion between "+ejercicio+" and "+(ejercicio-2)+")");
					pstm.setInt(1, ejercicio);
					pstm.executeUpdate();
					pstm.close();
					pstm = conn.prepareStatement("TRUNCATE TABLE dashboard_historia.mv_eventos_gc_group");
					pstm.executeUpdate();
					pstm.close();
					pstm = conn.prepareStatement("INSERT INTO dashboard_historia.mv_eventos_gc_group SELECT * FROM dashboard_historia.mv_eventos_gc_group_temp");
					pstm.executeUpdate();
					pstm.close();
					pstm = conn.prepareStatement("DROP TABLE IF EXISTS dashboard_historia.mv_eventos_gc_group_temp PURGE");
					pstm.executeUpdate();
					pstm.close();
					
					CLogger.writeConsole("Insertando valores a MV_EVENTOS_GC_GROUP");
					pstm = conn.prepareStatement("insert into table dashboard_historia.mv_eventos_group_gc "+
							"select "+ejercicio+" ano_publicacion, entidad_compradora, entidad_compradora_nombre, " + 
							"sum(case when mes_publicacion=1 and ano_publicacion="+(ejercicio-2)+" then 1 else 0 end) mes_1_ano_1, " + 
							"sum(case when mes_publicacion=1 and ano_publicacion="+(ejercicio-1)+" then 1 else 0 end) mes_1_ano_2, " + 
							"sum(case when mes_publicacion=1 and ano_publicacion="+ejercicio+" then 1 else 0 end) mes_1_ano_actual, " + 
							"sum(case when mes_publicacion=2 and ano_publicacion="+(ejercicio-2)+" then 1 else 0 end) mes_2_ano_1, " + 
							"sum(case when mes_publicacion=2 and ano_publicacion="+(ejercicio-1)+" then 1 else 0 end) mes_2_ano_2, " + 
							"sum(case when mes_publicacion=2 and ano_publicacion="+ejercicio+" then 1 else 0 end) mes_2_ano_actual, " + 
							"sum(case when mes_publicacion=3 and ano_publicacion="+(ejercicio-2)+" then 1 else 0 end) mes_3_ano_1, " + 
							"sum(case when mes_publicacion=3 and ano_publicacion="+(ejercicio-1)+" then 1 else 0 end) mes_3_ano_2, " + 
							"sum(case when mes_publicacion=3 and ano_publicacion="+ejercicio+" then 1 else 0 end) mes_3_ano_actual, " + 
							"sum(case when mes_publicacion=4 and ano_publicacion="+(ejercicio-2)+" then 1 else 0 end) mes_4_ano_1, " + 
							"sum(case when mes_publicacion=4 and ano_publicacion="+(ejercicio-1)+" then 1 else 0 end) mes_4_ano_2, " + 
							"sum(case when mes_publicacion=4 and ano_publicacion="+ejercicio+" then 1 else 0 end) mes_4_ano_actual, " + 
							"sum(case when mes_publicacion=5 and ano_publicacion="+(ejercicio-2)+" then 1 else 0 end) mes_5_ano_1, " + 
							"sum(case when mes_publicacion=5 and ano_publicacion="+(ejercicio-1)+" then 1 else 0 end) mes_5_ano_2, " + 
							"sum(case when mes_publicacion=5 and ano_publicacion="+ejercicio+" then 1 else 0 end) mes_5_ano_actual, " + 
							"sum(case when mes_publicacion=6 and ano_publicacion="+(ejercicio-2)+" then 1 else 0 end) mes_6_ano_1, " + 
							"sum(case when mes_publicacion=6 and ano_publicacion="+(ejercicio-1)+" then 1 else 0 end) mes_6_ano_2, " + 
							"sum(case when mes_publicacion=6 and ano_publicacion="+ejercicio+" then 1 else 0 end) mes_6_ano_actual, " + 
							"sum(case when mes_publicacion=7 and ano_publicacion="+(ejercicio-2)+" then 1 else 0 end) mes_7_ano_1, " + 
							"sum(case when mes_publicacion=7 and ano_publicacion="+(ejercicio-1)+" then 1 else 0 end) mes_7_ano_2, " + 
							"sum(case when mes_publicacion=7 and ano_publicacion="+ejercicio+" then 1 else 0 end) mes_7_ano_actual, " + 
							"sum(case when mes_publicacion=8 and ano_publicacion="+(ejercicio-2)+" then 1 else 0 end) mes_8_ano_1, " + 
							"sum(case when mes_publicacion=8 and ano_publicacion="+(ejercicio-1)+" then 1 else 0 end) mes_8_ano_2, " + 
							"sum(case when mes_publicacion=8 and ano_publicacion="+ejercicio+" then 1 else 0 end) mes_8_ano_actual, " + 
							"sum(case when mes_publicacion=9 and ano_publicacion="+(ejercicio-2)+" then 1 else 0 end) mes_9_ano_1, " + 
							"sum(case when mes_publicacion=9 and ano_publicacion="+(ejercicio-1)+" then 1 else 0 end) mes_9_ano_2, " + 
							"sum(case when mes_publicacion=9 and ano_publicacion="+ejercicio+" then 1 else 0 end) mes_9_ano_actual, " + 
							"sum(case when mes_publicacion=10 and ano_publicacion="+(ejercicio-2)+" then 1 else 0 end) mes_10_ano_1, " + 
							"sum(case when mes_publicacion=10 and ano_publicacion="+(ejercicio-1)+" then 1 else 0 end) mes_10_ano_2, " + 
							"sum(case when mes_publicacion=10 and ano_publicacion="+ejercicio+" then 1 else 0 end) mes_10_ano_actual, " + 
							"sum(case when mes_publicacion=11 and ano_publicacion="+(ejercicio-2)+" then 1 else 0 end) mes_11_ano_1, " + 
							"sum(case when mes_publicacion=11 and ano_publicacion="+(ejercicio-1)+" then 1 else 0 end) mes_11_ano_2, " + 
							"sum(case when mes_publicacion=11 and ano_publicacion="+ejercicio+" then 1 else 0 end) mes_11_ano_actual, " + 
							"sum(case when mes_publicacion=12 and ano_publicacion="+(ejercicio-2)+" then 1 else 0 end) mes_12_ano_1, " + 
							"sum(case when mes_publicacion=12 and ano_publicacion="+(ejercicio-1)+" then 1 else 0 end) mes_12_ano_2, " + 
							"sum(case when mes_publicacion=12 and ano_publicacion="+ejercicio+" then 1 else 0 end) mes_12_ano_actual " + 
							"from ( " + 
							"select ano_publicacion, mes_publicacion, entidad_compradora, entidad_compradora_nombre, nog_concurso, sum(monto) " + 
							"from mv_evento_gc " + 
							"where tipo_entidad = 4 " + 
							"and ano_publicacion between "+(ejercicio-2)+" and "+(ejercicio)+" "+ 
							"group by ano_publicacion, mes_publicacion, entidad_compradora, entidad_compradora_nombre, nog_concurso " + 
							") t1 " + 
							"group by entidad_compradora, entidad_compradora_nombre");
					pstm.executeUpdate();
					pstm.close();
					
					ret = true;
					rows = 0;
					first=true;
					pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_evento_gc_group(ano_publicacion,entidad_compradora, entidad_compradora_nombre," + 
							"mes_1_ano_1, mes_1_ano_2, mes_1_ano_actual,mes_2_ano_1, mes_2_ano_2, mes_2_ano_actual,mes_3_ano_1, mes_3_ano_2, mes_3_ano_actual,mes_4_ano_1, mes_4_ano_2, mes_4_ano_actual, "
							+ "mes_5_ano_1, mes_5_ano_2, mes_5_ano_actual,mes_6_ano_1, mes_6_ano_2, mes_6_ano_actual,mes_7_ano_1, mes_7_ano_2, mes_7_ano_actual,mes_8_ano_1, mes_8_ano_2, mes_8_ano_actual, "
							+ "mes_9_ano_1, mes_9_ano_2, mes_9_ano_actual,mes_10_ano_1, mes_10_ano_2, mes_10_ano_actual,mes_11_ano_1, mes_11_ano_2, mes_11_ano_actual,mes_12_ano_1, mes_12_ano_2, mes_12_ano_actual) "
							+ "values (?,?,?,"
							+ "?,?,?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?,?,?,?,?,?) ");
					
					pstm = conn.prepareStatement("SELECT * FROM dashboard_historia.mv_evento_gc_group where ano_publicacion = ?");
						pstm.setInt(1, ejercicio);
						pstm.setFetchSize(10000);
						rs = pstm.executeQuery();
						while(rs!=null && rs.next()){
							if(first){
								PreparedStatement pstm2 = CMemSQL.getConnection().prepareStatement("delete from mv_evento_gc_group where ano_publicacion =  ? ");
								pstm2.setInt(1, ejercicio);
								if (pstm2.executeUpdate()>0)
									CLogger.writeConsole("Registros eliminados");
								else
									CLogger.writeConsole("Sin registros para eliminar");	
								pstm2.close();
								first=false;
							}
							pstm1.setInt(1, rs.getInt("ano_publicacion"));
							pstm1.setInt(2, rs.getInt("entidad_compradora"));
							pstm1.setString(3, rs.getString("entidad_compradora_nombre"));
							pstm1.setInt(4, rs.getInt("mes_1_ano_1"));
							pstm1.setInt(5, rs.getInt("mes_1_ano_2"));
							pstm1.setInt(6, rs.getInt("mes_1_ano_actual"));
							pstm1.setInt(7, rs.getInt("mes_2_ano_1"));
							pstm1.setInt(8, rs.getInt("mes_2_ano_2"));
							pstm1.setInt(9, rs.getInt("mes_2_ano_actual"));
							pstm1.setInt(10, rs.getInt("mes_3_ano_1"));
							pstm1.setInt(11, rs.getInt("mes_3_ano_2"));
							pstm1.setInt(12, rs.getInt("mes_3_ano_actual"));
							pstm1.setInt(13, rs.getInt("mes_4_ano_1"));
							pstm1.setInt(14, rs.getInt("mes_4_ano_2"));
							pstm1.setInt(15, rs.getInt("mes_4_ano_actual"));
							pstm1.setInt(16, rs.getInt("mes_5_ano_1"));
							pstm1.setInt(17, rs.getInt("mes_5_ano_2"));
							pstm1.setInt(18, rs.getInt("mes_5_ano_actual"));
							pstm1.setInt(19, rs.getInt("mes_6_ano_1"));
							pstm1.setInt(20, rs.getInt("mes_6_ano_2"));
							pstm1.setInt(21, rs.getInt("mes_6_ano_actual"));
							pstm1.setInt(22, rs.getInt("mes_7_ano_1"));
							pstm1.setInt(23, rs.getInt("mes_7_ano_2"));
							pstm1.setInt(24, rs.getInt("mes_7_ano_actual"));
							pstm1.setInt(25, rs.getInt("mes_8_ano_1"));
							pstm1.setInt(26, rs.getInt("mes_8_ano_2"));
							pstm1.setInt(27, rs.getInt("mes_8_ano_actual"));
							pstm1.setInt(28, rs.getInt("mes_9_ano_1"));
							pstm1.setInt(29, rs.getInt("mes_9_ano_2"));
							pstm1.setInt(30, rs.getInt("mes_9_ano_actual"));
							pstm1.setInt(31, rs.getInt("mes_10_ano_1"));
							pstm1.setInt(32, rs.getInt("mes_10_ano_2"));
							pstm1.setInt(33, rs.getInt("mes_10_ano_actual"));
							pstm1.setInt(34, rs.getInt("mes_11_ano_1"));
							pstm1.setInt(35, rs.getInt("mes_11_ano_2"));
							pstm1.setInt(36, rs.getInt("mes_11_ano_actual"));
							pstm1.setInt(37, rs.getInt("mes_12_ano_1"));
							pstm1.setInt(38, rs.getInt("mes_12_ano_2"));
							pstm1.setInt(39, rs.getInt("mes_12_ano_actual"));
							
							pstm1.addBatch();
							rows++;
							
							if((rows % 10000) == 0)
								pstm1.executeBatch();
						}
						pstm1.executeBatch();
						rs.close();
						pstm.close();
					
					CLogger.writeConsole("Records escritos Totales: "+rows);
					pstm1.close();
				}
				
			}					
				
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 2: CEjecucionPresupuestaria.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
	
}
