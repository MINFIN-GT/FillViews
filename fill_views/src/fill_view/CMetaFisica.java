package fill_view;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.joda.time.DateTime;

import utilities.CLogger;

public class CMetaFisica {
	
	public static boolean loadMetasFisicas(Connection conn, boolean historico, boolean des){
		DateTime date = new DateTime();
		String query = "select actividad, cantidad, codigo_meta, descripcion, ejercicio, ejercicio_meta, entidad, estado, fecha_fin, fecha_inicio, "
				+ "habilitado, nivel, nivel_meta, obra, programa, proyecto, snip, subprograma, unidad_ejecutora, unidad_medida FROM sf_meta "+
				(!historico ? "	   WHERE ejercicio = " + date.getYear() + " " : "" );
		boolean ret = false;
		try{
			if(!conn.isClosed()){
				ResultSet rs = conn.prepareStatement(query).executeQuery();
				boolean bconn = (!des) ? CMemSQL.connect() : CMemSQL.connectdes();
				if(rs!=null && bconn){
					ret = true;
					int rows = 0;
					CLogger.writeConsole("CMetaFisica:");
					SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
					PreparedStatement pstm;
					boolean first=true;
					while(rs.next()){
						if(first){
							pstm = CMemSQL.getConnection().prepareStatement("delete from meta_fisica "
									+ (!historico ? " where ejercicio=" + date.getYear() : ""))  ;
							if (pstm.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");	
							pstm.close();
							first=false;
						}
						pstm = CMemSQL.getConnection().prepareStatement("Insert INTO meta_fisica(actividad, cantidad, codigo_meta, descripcion, ejercicio, ejercicio_meta, entidad, estado, fecha_fin, fecha_inicio," + 
								"habilitado, nivel, nivel_meta, obra, programa, proyecto, snip, subprograma, unidad_ejecutora, unidad_medida) "
								+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ");
						pstm.setInt(1, rs.getInt("actividad"));
						pstm.setInt(2,rs.getInt("cantidad"));
						pstm.setInt(3,rs.getInt("codigo_meta"));
						pstm.setString(4, rs.getString("descripcion"));
						pstm.setInt(5,rs.getInt("ejercicio"));
						pstm.setInt(6,rs.getInt("ejercicio_meta"));
						pstm.setInt(7,rs.getInt("entidad"));
						pstm.setString(8, rs.getString("estado"));
						Date parsedDate = rs.getString("fecha_fin")!=null ? dateFormat.parse(rs.getString("fecha_fin")) : null;
						pstm.setTimestamp(9, parsedDate!=null ? new Timestamp(parsedDate.getTime()) : null);
						parsedDate = rs.getString("fecha_inicio")!=null ? dateFormat.parse(rs.getString("fecha_inicio")) : null;
						pstm.setTimestamp(10, parsedDate!=null ? new Timestamp(parsedDate.getTime()) : null);
						pstm.setString(11, rs.getString("habilitado"));
						pstm.setInt(12,rs.getInt("nivel"));
						pstm.setInt(13,rs.getInt("nivel_meta"));
						pstm.setInt(14,rs.getInt("obra"));
						pstm.setInt(15,rs.getInt("programa"));
						pstm.setInt(16,rs.getInt("proyecto"));
						pstm.setInt(17,rs.getInt("snip"));
						pstm.setInt(18,rs.getInt("subprograma"));
						pstm.setInt(19,rs.getInt("unidad_ejecutora"));
						pstm.setInt(20,rs.getInt("unidad_medida"));
						
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
			CLogger.writeFullConsole("Error 1: CMetaFisica.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
}
