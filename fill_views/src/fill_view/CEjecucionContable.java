package fill_view;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import utilities.CLogger;

public class CEjecucionContable {
	public static boolean loadEjecucionAnticipos(Connection conn, Integer ejercicio){
		boolean ret = false;
		try{
			if( !conn.isClosed() && CMemSQL.connect()){
				CLogger.writeConsole("CEjecucionPresupuestaria Entidades (Ejercicio "+ejercicio+"):");
				PreparedStatement pstm;
				CLogger.writeConsole("Copiando historia - MV_ANTICIPO_CONTABLE");
				pstm = conn.prepareStatement("TRUNCATE TABLE dashboard.mv_anticipo_contable");
				pstm.executeUpdate();
				pstm.close();
				pstm = conn.prepareStatement("INSERT INTO dashboard.mv_anticipo_contable SELECT * FROM dashboard_historia.mv_anticipo_contable where ejercicio <> ?");
				pstm.setInt(1, ejercicio);
				pstm.executeUpdate();
				pstm.close();
				
				CLogger.writeConsole("Insertando valores a MV_ANTICIPO_CONTABLE");
				pstm =conn.prepareStatement("insert into table dashboard.mv_anticipo_contable "+
						"select t.ejercicio, t.mes, cr.clase_registro,  cr.descripcion,  " + 
						"sum(ch.monto_contable) monto " + 
						"from dashboard.tiempo t " + 
						"left outer join sicoinprod.co_clases_registro cr  " + 
						"on (t.ejercicio = cr.ejercicio and cr.clase_registro in ('EIA','EIAP','EIC','EICO','EID','EIE','EIF','EIP','EIR','FRA','FRC','FRR', 'NDB'))  " + 
						"left outer join sicoinprod.co_contabilidad_hoja ch  " + 
						"on(t.ejercicio = ch.ejercicio and t.mes = month(ch.fec_aprobado) and ch.ejercicio = cr.ejercicio and ch.clase_registro = cr.clase_registro " + 
						"  and ch.estado = 'PAGADO' and ch.aprobado = 'S' and ch.revertido = 'N' " + 
						")  " + 
						"where t.dia=1  " + 
						"and t.ejercicio = ? " + 
						"group by t.ejercicio, t.mes, cr.clase_registro, cr.descripcion");
				pstm.setInt(1, ejercicio);
				pstm.executeUpdate();
				
				boolean bconn =  CMemSQL.connect();
				CLogger.writeConsole("Cargando datos a cache de MV_ANTICIPO_CONTABLE");
				if(bconn){
					CMemSQL.getConnection().setAutoCommit(false);
					ret = true;
					int rows = 0;
					boolean first=true;
					PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_anticipo_contable(ejercicio, mes, clase_registro, descripcion, monto) "
							+ "values (?,?,?,?,?) ");
					pstm = conn.prepareStatement("SELECT * FROM dashboard.mv_anticipo_contable where ejercicio = ? ");
					pstm.setInt(1, ejercicio);
					pstm.setFetchSize(1000);
					ResultSet rs = pstm.executeQuery();
					while(rs!=null && rs.next()){
						if(first){
							PreparedStatement pstm2 = CMemSQL.getConnection().prepareStatement("delete from mv_anticipo_contable where ejercicio = ? ");
							pstm2.setInt(1, ejercicio);
							if (pstm2.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");	
							pstm2.close();
							first=false;
						}
						pstm1.setInt(1, rs.getInt("ejercicio"));
						pstm1.setInt(2, rs.getInt("mes"));
						pstm1.setString(3, rs.getString("clase_registro"));
						pstm1.setString(4, rs.getString("descripcion"));
						pstm1.setDouble(5, rs.getDouble("monto"));
						pstm1.addBatch();
						rows++;
						if((rows % 1000) == 0){
							pstm1.executeBatch();
							CMemSQL.getConnection().commit();
						}
					}
					pstm1.executeBatch();
					rs.close();
					pstm.close();
					CMemSQL.getConnection().commit();
					
					CLogger.writeConsole("Records escritos Totales: "+rows);
				}
			}
		}
		catch(Exception e) {
			CLogger.writeFullConsole("Error 1: CEjecucionContable.class", e);
		}
		finally {
			CMemSQL.close();
		}
		return ret;
	}
}
