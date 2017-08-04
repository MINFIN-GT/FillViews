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
				pstm = conn.prepareStatement("INSERT INTO dashboard.mv_evento_gc SELECT * FROM dashboard_historia.mv_evento_gc WHERE YEAR(fecha_publicacion) < ?");
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
					
					pstm = conn.prepareStatement("SELECT * FROM dashboard.mv_evento_gc where YEAR(fecha_publicacion) = ?");
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
