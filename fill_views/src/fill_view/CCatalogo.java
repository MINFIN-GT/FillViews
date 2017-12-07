package fill_view;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import utilities.CLogger;

public class CCatalogo {

	
	public static boolean loadCatalogos(Connection conn,Integer ejercicio){
		boolean ret=true;
		try{
			CLogger.writeConsole("CCatalogo");
			if(CMemSQL.connect()){
				CLogger.writeConsole("Elimininandos catalogos actuales");
				PreparedStatement pstm;
				pstm = CMemSQL.getConnection().prepareStatement("DELETE FROM minfin.cp_recursos WHERE ejercicio=?");
				pstm.setInt(1, ejercicio);
				pstm.executeUpdate();
				pstm.close();
				pstm = CMemSQL.getConnection().prepareStatement("DELETE FROM minfin.cp_recursos_auxiliares WHERE ejercicio=?");
				pstm.setInt(1, ejercicio);
				pstm.executeUpdate();
				pstm.close();
				PreparedStatement ps=conn.prepareStatement("SELECT * FROM sicoinprod.cp_recursos WHERE ejercicio = ?");
				ps.setInt(1, ejercicio);
				ResultSet rs = ps.executeQuery();
				pstm = CMemSQL.getConnection().prepareStatement("INSERT INTO minfin.cp_recursos(ejercicio, recurso, nombre, grupo_ingreso, hoja, clase, seccion, grupo, contenido) VALUES(?,?,?,?,?,?,?,?,?)");
				int rows=0;
				while(rs.next()){
					pstm.setInt(1, rs.getInt("ejercicio"));
					pstm.setInt(2, rs.getInt("recurso"));
					pstm.setString(3, rs.getString("nombre"));
					pstm.setInt(4, rs.getInt("grupo_ingreso"));
					pstm.setString(5, rs.getString("hoja"));
					pstm.setInt(6, rs.getInt("clase"));
					pstm.setInt(7, rs.getInt("seccion"));
					pstm.setInt(8, rs.getInt("grupo"));
					pstm.setString(9, rs.getString("contenido"));
					pstm.addBatch();
					
					rows++;
					if((rows % 100) == 0)
						pstm.executeBatch();
				}
				pstm.executeBatch();
				pstm.close();
				rs.close();
				ps.close();
				CLogger.writeConsole("Tabla cp_recursos cargada con éxito.  "+ rows + " filas insertadas.");
				
				ps=conn.prepareStatement("SELECT * FROM sicoinprod.cp_recursos_auxiliares WHERE ejercicio = ?");
				ps.setInt(1, ejercicio);
				rs = ps.executeQuery();
				pstm = CMemSQL.getConnection().prepareStatement("INSERT INTO minfin.cp_recursos_auxiliares(ejercicio, entidad, unidad_ejecutora, recurso, recurso_auxiliar, nombre, sigla, recaudacion) VALUES(?,?,?,?,?,?,?,?)");
				rows=0;
				while(rs.next()){
					pstm.setInt(1, rs.getInt("ejercicio"));
					pstm.setInt(2, rs.getInt("entidad"));
					pstm.setInt(3, rs.getInt("unidad_ejecutora"));
					pstm.setInt(4, rs.getInt("recurso"));
					pstm.setInt(5, rs.getInt("recurso_auxiliar"));
					pstm.setString(6, rs.getString("nombre"));
					pstm.setString(7, rs.getString("sigla"));
					pstm.setInt(8, rs.getInt("recaudacion"));
					pstm.addBatch();
					
					rows++;
					if((rows % 100) == 0)
						pstm.executeBatch();
				}
				pstm.executeBatch();
				pstm.close();
				rs.close();
				ps.close();
				CLogger.writeConsole("Tabla cp_recursos_auxiliares cargada con éxito.  "+ rows + " filas insertadas.");
				
				CMemSQL.close();
			}
		}
		catch(Exception e){
			ret=false;
			CLogger.writeFullConsole("Error al cargar catalogos", e);
		}
		return ret;
	}
}
