package fill_view;

import java.sql.Connection;
import java.sql.PreparedStatement;

import utilities.CLogger;

public class CSnip {
	public static boolean loadSnip(Connection conn){
		boolean ret = true;
		try{
			CLogger.writeConsole("CSnip");
			CLogger.writeConsole("Elminiando la data actual de MV_SNIP");
			PreparedStatement pstm;
			pstm = conn.prepareStatement("TRUNCATE TABLE dashboard.mv_snip");
			pstm.executeUpdate();
			pstm.close();
			
			CLogger.writeConsole("Insertando valores a MV_SNIP");
			pstm = conn.prepareStatement("INSERT INTO dashboard.mv_snip "+
				"select " + 
				"p.snip, p.no_proyecto, p.ejercicio, p.entidad, p.unidad_ejecutora,  " + 
				"regexp_replace(regexp_replace(p.nombre_proyecto,\",\",\"\\,\"),\"\\\\r\\\\n|\\\\n\",\"<br>\") nombre_proyecto, " + 
				"regexp_replace(regexp_replace(p.descripcion_detallada,\",\",\"\\,\"),\"\\\\r\\\\n|\\\\n\",\"<br>\") descripcion_detallada,  " + 
				"p.tipo_proyecto, p.geografico, p.fecha_registro, p.estado, p.etapa, p.activado, p.fecha_modifica, " + 
				"pe.programa, pe.subprograma, pe.proyecto, pe.actividad, pe.obra, " + 
				"pp.geografico partida_geografico, pp.renglon, pp.fuente, pp.organismo, pp.monto_solicitado, pp.monto_aprobado, " + 
				"pae.clase_registro, pae.fecha fecha_partida_ejecucion, pae.tipo_operacion, pae.monto monto_ejecucion, pae.clase_modificacion, " + 
				"ma.meta meta_anual,  " + 
				"regexp_replace(regexp_replace(ma.descripcion,\",\",\"\\,\"),\"\\\\r\\\\n|\\\\n\",\" \") meta_anual_descripcion,  " + 
				"ma.unidad_medida meta_anual_unidad_medida, " + 
				"mg.meta meta_global,  " + 
				"regexp_replace(regexp_replace(mg.descripcion,\",\",\"\\,\"),\"\\\\r\\\\n|\\\\n\",\" \") meta_global_descripcion,  " + 
				"mg.unidad_medida meta_glogal_unidad_medida " + 
				"from siges.snip_proyecto p left outer join siges.snip_proyecto_estructura pe " + 
				"	on ( " + 
				"		p.ejercicio = pe.ejercicio " + 
				"		and p.entidad = pe.entidad " + 
				"		and p.unidad_ejecutora = pe.unidad_ejecutora " + 
				"		and p.snip = pe.snip	 " + 
				"		and p.gestion = pe.gestion " + 
				"	) left outer join siges.snip_proyecto_partida pp " + 
				"	on( " + 
				"		p.ejercicio = pp.ejercicio " + 
				"		and p.entidad = pp.entidad " + 
				"		and p.unidad_ejecutora = pp.unidad_ejecutora " + 
				"		and p.snip = pp.snip	 " + 
				"		and p.gestion = pp.gestion " + 
				"		and pe.programa = pp.programa " + 
				"		and pe.subprograma = pp.subprograma " + 
				"		and pe.proyecto = pp.proyecto " + 
				"		and pe.obra = pp.obra " + 
				"		and pe.actividad = pp.actividad " + 
				"		and pe.gestion = pp.gestion " + 
				"	) left outer join siges.snip_partida_ejecucion pae " + 
				"	on( " + 
				"		p.ejercicio = pae.ejercicio " + 
				"		and p.entidad = pae.entidad " + 
				"		and p.unidad_ejecutora = pae.unidad_ejecutora " + 
				"		and p.snip = pae.snip	 " + 
				"		and p.gestion = pae.gestion " + 
				"		and pp.programa = pae.programa " + 
				"		and pp.subprograma = pae.subprograma " + 
				"		and pp.proyecto = pae.proyecto " + 
				"		and pp.obra = pae.obra " + 
				"		and pp.actividad = pae.actividad " + 
				"		and pp.geografico = pae.geografico " + 
				"		and pp.fuente = pae.fuente " + 
				"		and pp.gestion = pae.gestion " + 
				"	) left outer join siges.snip_meta_anual ma " + 
				"	on( " + 
				"		p.snip = ma.snip " + 
				"		and p.ejercicio = ma.ejercicio " + 
				"		and ma.restrictiva = 'N' " + 
				"	) left outer join siges.snip_meta_global mg " + 
				"	on( " + 
				"		p.snip = mg.snip " + 
				"	)");
			pstm.executeUpdate();
			pstm.close();
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 1: CSnip.class", e);
		}
		finally{
		
		}
		return ret;
	}
}
