package fill_view;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.joda.time.DateTime;

import utilities.CLogger;

public class CUnidadMedida {
	public static boolean loadUnidadesMedida(Connection conn, boolean historico, boolean des){
		DateTime date = new DateTime();
		String query = "SELECT clasificacion, codigo , ejercicio , grupo, nombre FROM fp_unidad_medida " +
				(!historico ? "	  WHERE ejercicio = " + date.getYear() + " " : "" );
		boolean ret = false;
		try{
			if(!conn.isClosed()){
				ResultSet rs = conn.prepareStatement(query).executeQuery();
				boolean bconn = (!des) ? CMemSQL.connect() : CMemSQL.connectdes();
				if(rs!=null && bconn){
					ret = true;
					int rows = 0;
					CLogger.writeConsole("CUnidadMedida:");
					PreparedStatement pstm;
					boolean first=true;
					while(rs.next()){
						if(first){
							pstm = CMemSQL.getConnection().prepareStatement("delete from fp_unidad_medida "
									+ (!historico ? " where ejercicio=" + date.getYear() : ""))  ;
							if (pstm.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");	
							pstm.close();
							first=false;
						}
						pstm = CMemSQL.getConnection().prepareStatement("Insert INTO fp_unidad_medida(clasificacion,codigo,ejercicio,grupo,nombre) "
								+ "values (?,?,?,?,?) ");
						pstm.setInt(1, rs.getInt("clasificacion"));
						pstm.setInt(2,rs.getInt("codigo"));		
						pstm.setInt(3, rs.getInt("ejercicio"));
						pstm.setInt(4, rs.getInt("grupo"));
						pstm.setString(5, rs.getString("nombre"));
						ret = ret && pstm.executeUpdate()>0;
						rows++;
						if((rows % 100) == 0)
							CLogger.writeConsole(String.join("Records escritos: ",String.valueOf(rows)));
						pstm.close();
					}		
				}
			}
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 1: CUnidadMedidad.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
}
