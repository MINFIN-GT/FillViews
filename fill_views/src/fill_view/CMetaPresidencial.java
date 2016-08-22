package fill_view;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.joda.time.DateTime;

import utilities.CLogger;

public class CMetaPresidencial {
	public static boolean loadEjecucionFisicaFinanciera(boolean descentralizadas){
		DateTime date = new DateTime();
		String query = "";
		boolean ret = false;		
		try{
			Connection conn = descentralizadas ? CHive.openConnectiondes() : CHive.openConnection();			
			if(!conn.isClosed()&&CMemSQL.connect()){
				ResultSet rs = conn.prepareStatement(query).executeQuery();
				if(rs!=null){
					ret = true;
					int rows = 0;
					CLogger.writeConsole("CMetaPresidencial (loadEjecucionFisica "+ (descentralizadas ? "Descentralizadas" : "Centralizadas") +"):");
					PreparedStatement pstm;
					while(rs.next()){						
						if (rs.first()&&!descentralizadas){							
							pstm = CMemSQL.getConnection().prepareStatement("delete from meta_presidencial "
									+" where ejercicio=" + date.getYear())  ;
							if (pstm.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");	
							pstm.close();
							ret=true;
						}
						
						pstm = CMemSQL.getConnection().prepareStatement("Insert INTO meta_presidencial(ejercicio,mes,id,nombre, "
								+  "entidad,entidad_nombre,unidad_ejecutora,unidad_ejecutora_nombre,programa,programa_nombre "+
							       ",subprograma,subprograma_nombre,proyecto,proyecto_nombre,actividad,obra,actividad_obra_nombre, "+
							       "renglon,renglon_nombre,mf_id,mf_nombre,mf_unidad_medida,mf_unidad_medida_nombre, "+
							       "vigente,ejecutado,meta,meta_avanzado) "+
									"values (?,?,?,?,?,"
										  + "?,?,?,?,?,"
										  + "?,?,?,?,?,"
										  + "?,?,?,?,?,"
										  + "?,?,?,?,?,"
										  + "?) ");
						pstm.setInt(1, rs.getInt("ejercicio"));
						pstm.setInt(2, rs.getInt("mes"));		
						pstm.setInt(3, rs.getInt("id"));
						pstm.setString(4, rs.getString("nombre"));
						pstm.setInt(5, rs.getInt("entidad"));
						pstm.setString(6, rs.getString("entidad_nombre"));
						pstm.setInt(7, rs.getInt("unidad_ejecutora"));
						pstm.setString(8, rs.getString("unidad_ejecutora_nombre"));
						pstm.setInt(9, rs.getInt("programa"));
						pstm.setString(10, rs.getString("programa_nombre"));
						pstm.setInt(11, rs.getInt("subprograma"));
						pstm.setString(12, rs.getString("subprograma_nombre"));
						pstm.setInt(13,rs.getInt("proyecto"));
						pstm.setString(14, rs.getString("proyecto_nombre"));
						pstm.setInt(15,rs.getInt("actividad"));
						pstm.setInt(16,rs.getInt("obra"));
						pstm.setString(17, rs.getString("actividad_nombre"));
						pstm.setInt(18,rs.getInt("renglon"));
						pstm.setString(19, rs.getString("renglon_nombre"));
						pstm.setInt(20,rs.getInt("mf_id"));
						pstm.setString(21, rs.getString("mf_nombre"));
						pstm.setInt(22,rs.getInt("mf_unidad_medida"));
						pstm.setString(22, rs.getString("mf_unidad_medida_nombre"));
						pstm.setDouble(23, rs.getDouble("vigente"));
						pstm.setDouble(24, rs.getDouble("ejecutado"));
						pstm.setDouble(25, rs.getDouble("meta"));
						pstm.setDouble(26, rs.getDouble("meta_avanzado"));
						ret = ret && pstm.executeUpdate()>0;
						rows++;
						if((rows % 10) == 0)
							CLogger.writeConsole(String.join("Records escritos: ",String.valueOf(rows)));
						pstm.close();
					}		
				}
			}
			conn.close();
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 1: CMetaPresidencial.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
}
