package fill_view;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.joda.time.DateTime;

import utilities.CLogger;

public class CMeta {
	
	static class st_meta_ep{
		int meta_presidencial_id;
		String meta_nombre;
		int entidad;
		String entidad_nombre;
		int unidad_ejecutora;
		String unidad_ejecutora_nombre;
		int programa;
		String programa_nombre;
		int subprograma;
		String subprograma_nombre;
		int proyecto;
		String proyecto_nombre;
		int actividad;
		String actividad_nombre;
		int obra;
		String obra_nombre;
		int renglon;
		String renlgon_nombre;
		double asignado;
		double vigente;
		double ejecutado;
	}
	
	
	public static boolean calcularMv_meta_presidencial(Connection conn, int ejercicio){
		boolean ret = false;
		DateTime now = new DateTime();
		String sql="select t.ejercicio, t.mes, mp.id, mp.nombre, em.nombre nombre_entidad,uem.nombre nombre_unidad_ejecutora, pm.nom_estructura nombre_programa, spm.nom_estructura nombre_subprograma, prm.nom_estructura nombre_proyecto, ao.nom_estructura nombre_actividad_obra, " + 
				"         ep.entidad, ep.unidad_ejecutora, ep.programa, ep.subprograma, ep.proyecto, ep.actividad, ep.obra, mm.codigo_meta,  " + 
				"         v.renglon, rm.nombre nombre_renglon, v.asignado, v.vigente_1, v.vigente_2, v.vigente_3, v.vigente_4, v.vigente_5, v.vigente_6, v.vigente_7, v.vigente_8, v.vigente_9, v.vigente_10, v.vigente_11, v.vigente_12, " + 
				"         ejecutado.gasto_total, m.cantidad meta_cantidad, m.adicion meta_adicion, m.disminucion meta_disminucion, m.descripcion meta_descripcion, um.nombre meta_medida, ef.meta_cantidad_unidades_avance " + 
				"from dashboard.tiempo t, " + 
				"dashboard.meta_presidencial_ep_meta mm, sicoinprod.sf_meta m, " + 
				"dashboard.meta_presidencial_ep ep left outer join ( " + 
				" " + 
				"select p.ejercicio,p.entidad, p.unidad_ejecutora, p.renglon, p.programa, p.subprograma, p.proyecto, p.actividad, p.obra, sum(p.asignado) asignado,(sum(p.asignado) + sum(p.adicion) + sum(p.disminucion) + sum(p.traspaso_p)+ sum(p.traspaso_n)+ sum(p.transferencia_p) + sum(p.transferencia_n)) vigente_actual,  " + 
				"				sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) vigente_1,  " + 
				"				sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) +  " + 
				"						   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) vigente_2,   " + 
				"				sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) +  " + 
				"						   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) +   " + 
				"						   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) vigente_3,   " + 
				"				sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) +   " + 
				"						   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) +   " + 
				"						   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) +   " + 
				"						   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) vigente_4,   " + 
				"				sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) +   " + 
				"						   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) +   " + 
				"			   		   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) +   " + 
				"						   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) +   " + 
				"					   	   (sum(p.adicion_05) + sum(p.disminucion_05) + sum(p.traspaso_p05)+ sum(p.traspaso_n05)+ sum(p.transferencia_p05) + sum(p.transferencia_n05) ) vigente_5,   " + 
				"				sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum( " + 
				"				p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) +   " + 
				"						   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) +   " + 
				"						   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) +   " + 
				"						   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) +   " + 
				"					   	   (sum(p.adicion_05) + sum(p.disminucion_05) + sum(p.traspaso_p05)+ sum(p.traspaso_n05)+ sum(p.transferencia_p05) + sum(p.transferencia_n05) ) +   " + 
				"						   (sum(p.adicion_06) + sum(p.disminucion_06) + sum(p.traspaso_p06)+ sum(p.traspaso_n06)+ sum(p.transferencia_p06) + sum(p.transferencia_n06) ) vigente_6,   " + 
				"				sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) +   " + 
				"						   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) +   " + 
				"						   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) +   " + 
				"						   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) +   " + 
				"					   	   (sum(p.adicion_05) + sum(p.disminucion_05) + sum(p.traspaso_p05)+ sum(p.traspaso_n05)+ sum(p.transferencia_p05) + sum(p.transferencia_n05) ) +   " + 
				"						   (sum(p.adicion_06) + sum(p.disminucion_06) + sum(p.traspaso_p06)+ sum(p.traspaso_n06)+ sum(p.transferencia_p06) + sum(p.transferencia_n06) ) +   " + 
				"						   (sum(p.adicion_07) + sum(p.disminucion_07) + sum(p.traspaso_p07)+ sum(p.traspaso_n07)+ sum(p.transferencia_p07) + sum(p.transferencia_n07) ) vigente_7,   " + 
				"				sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) +   " + 
				"						   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) +   " + 
				"						   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) +   " + 
				"						   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) +   " + 
				"					   	   (sum(p.adicion_05) + sum(p.disminucion_05) + sum(p.traspaso_p05)+ sum(p.traspaso_n05)+ sum(p.transferencia_p05) + sum(p.transferencia_n05) ) +   " + 
				"						   (sum(p.adicion_06) + sum(p.disminucion_06) + sum(p.traspaso_p06)+ sum(p.traspaso_n06)+ sum(p.transferencia_p06) + sum(p.transferencia_n06) ) +   " + 
				"						   (sum(p.adicion_07) + sum(p.disminucion_07) + sum(p.traspaso_p07)+ sum(p.traspaso_n07)+ sum(p.transferencia_p07) + sum(p.transferencia_n07) ) +   " + 
				"						   (sum(p.adicion_08) + sum(p.disminucion_08) + sum(p.traspaso_p08)+ sum(p.traspaso_n08)+ sum(p.transferencia_p08) + sum(p.transferencia_n08) ) vigente_8,   " + 
				"				sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) +   " + 
				"						   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) +   " + 
				"						   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) +   " + 
				"						   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) +   " + 
				"					   	   (sum(p.adicion_05) + sum(p.disminucion_05) + sum(p.traspaso_p05)+ sum(p.traspaso_n05)+ sum(p.transferencia_p05) + sum(p.transferencia_n05) ) +   " + 
				"						   (sum(p.adicion_06) + sum(p.disminucion_06) + sum(p.traspaso_p06)+ sum(p.traspaso_n06)+ sum(p.transferencia_p06) + sum(p.transferencia_n06) ) +   " + 
				"						   (sum(p.adicion_07) + sum(p.disminucion_07) + sum(p.traspaso_p07)+ sum(p.traspaso_n07)+ sum(p.transferencia_p07) + sum(p.transferencia_n07) ) +   " + 
				"						   (sum(p.adicion_08) + sum(p.disminucion_08) + sum(p.traspaso_p08)+ sum(p.traspaso_n08)+ sum(p.transferencia_p08) + sum(p.transferencia_n08) ) +   " + 
				"						   (sum(p.adicion_09) + sum(p.disminucion_09) + sum(p.traspaso_p09)+ sum(p.traspaso_n09)+ sum(p.transferencia_p09) + sum(p.transferencia_n09) ) vigente_9,   " + 
				"				sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) +   " + 
				"						   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) +   " + 
				"						   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) +   " + 
				"						   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) +   " + 
				"					   	   (sum(p.adicion_05) + sum(p.disminucion_05) + sum(p.traspaso_p05)+ sum(p.traspaso_n05)+ sum(p.transferencia_p05) + sum(p.transferencia_n05) ) +   " + 
				"						   (sum(p.adicion_06) + sum(p.disminucion_06) + sum(p.traspaso_p06)+ sum(p.traspaso_n06)+ sum(p.transferencia_p06) + sum(p.transferencia_n06) ) +   " + 
				"						   (sum(p.adicion_07) + sum(p.disminucion_07) + sum(p.traspaso_p07)+ sum(p.traspaso_n07)+ sum(p.transferencia_p07) + sum(p.transferencia_n07) ) +   " + 
				"						   (sum(p.adicion_08) + sum(p.disminucion_08) + sum(p.traspaso_p08)+ sum(p.traspaso_n08)+ sum(p.transferencia_p08) + sum(p.transferencia_n08) ) +   " + 
				"						   (sum(p.adicion_09) + sum(p.disminucion_09) + sum(p.traspaso_p09)+ sum(p.traspaso_n09)+ sum(p.transferencia_p09) + sum(p.transferencia_n09) ) +   " + 
				"						   (sum(p.adicion_10) + sum(p.disminucion_10) + sum(p.traspaso_p10)+ sum(p.traspaso_n10)+ sum(p.transferencia_p10) + sum(p.transferencia_n10) ) vigente_10,   " + 
				"				sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) +   " + 
				"						   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) +   " + 
				"						   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) +   " + 
				"						   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) +   " + 
				"					   	   (sum(p.adicion_05) + sum(p.disminucion_05) + sum(p.traspaso_p05)+ sum(p.traspaso_n05)+ sum(p.transferencia_p05) + sum(p.transferencia_n05) ) +   " + 
				"						   (sum(p.adicion_06) + sum(p.disminucion_06) + sum(p.traspaso_p06)+ sum(p.traspaso_n06)+ sum(p.transferencia_p06) + sum(p.transferencia_n06) ) +   " + 
				"						   (sum(p.adicion_07) + sum(p.disminucion_07) + sum(p.traspaso_p07)+ sum(p.traspaso_n07)+ sum(p.transferencia_p07) + sum(p.transferencia_n07) ) +   " + 
				"						   (sum(p.adicion_08) + sum(p.disminucion_08) + sum(p.traspaso_p08)+ sum(p.traspaso_n08)+ sum(p.transferencia_p08) + sum(p.transferencia_n08) ) +   " + 
				"						   (sum(p.adicion_09) + sum(p.disminucion_09) + sum(p.traspaso_p09)+ sum(p.traspaso_n09)+ sum(p.transferencia_p09) + sum(p.transferencia_n09) ) +   " + 
				"						   (sum(p.adicion_10) + sum(p.disminucion_10) + sum(p.traspaso_p10)+ sum(p.traspaso_n10)+ sum(p.transferencia_p10) + sum(p.transferencia_n10) ) +   " + 
				"			  		   (sum(p.adicion_11) + sum(p.disminucion_11) + sum(p.traspaso_p11)+ sum(p.traspaso_n11)+ sum(p.transferencia_p11) + sum(p.transferencia_n11) ) vigente_11,   " + 
				"				sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) +   " + 
				"						   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) +   " + 
				"						   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) +   " + 
				"						   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) +   " + 
				"					   	   (sum(p.adicion_05) + sum(p.disminucion_05) + sum(p.traspaso_p05)+ sum(p.traspaso_n05)+ sum(p.transferencia_p05) + sum(p.transferencia_n05) ) +   " + 
				"						   (sum(p.adicion_06) + sum(p.disminucion_06) + sum(p.traspaso_p06)+ sum(p.traspaso_n06)+ sum(p.transferencia_p06) + sum(p.transferencia_n06) ) +   " + 
				"						   (sum(p.adicion_07) + sum(p.disminucion_07) + sum(p.traspaso_p07)+ sum(p.traspaso_n07)+ sum(p.transferencia_p07) + sum(p.transferencia_n07) ) +   " + 
				"						   (sum(p.adicion_08) + sum(p.disminucion_08) + sum(p.traspaso_p08)+ sum(p.traspaso_n08)+ sum(p.transferencia_p08) + sum(p.transferencia_n08) ) +   " + 
				"						   (sum(p.adicion_09) + sum(p.disminucion_09) + sum(p.traspaso_p09)+ sum(p.traspaso_n09)+ sum(p.transferencia_p09) + sum(p.transferencia_n09) ) +   " + 
				"						   (sum(p.adicion_10) + sum(p.disminucion_10) + sum(p.traspaso_p10)+ sum(p.traspaso_n10)+ sum(p.transferencia_p10) + sum(p.transferencia_n10) ) +   " + 
				"						   (sum(p.adicion_11) + sum(p.disminucion_11) + sum(p.traspaso_p11)+ sum(p.traspaso_n11)+ sum(p.transferencia_p11) + sum(p.transferencia_n11) ) +   " + 
				"						   (sum(p.adicion_12) + sum(p.disminucion_12) + sum(p.traspaso_p12)+ sum(p.traspaso_n12)+ sum(p.transferencia_p12) + sum(p.transferencia_n12) ) vigente_12   " + 
				"				from sicoinprod.EG_F6_PARTIDAS p  " + 
				"				where p.ejercicio= ?  " + 
				"				group by p.ejercicio,p.entidad,p.unidad_ejecutora, p.programa,p.subprograma, p.proyecto, p.actividad, p.obra, p.renglon " + 
				") v " + 
				"on ( " + 
				"  ep.ejercicio = v.ejercicio " + 
				"  and ep.entidad = v.entidad " + 
				"  and ep.unidad_ejecutora = v.unidad_ejecutora " + 
				"  and ep.programa = v.programa " + 
				"  and ep.subprograma = v.subprograma " + 
				"  and ep.proyecto = v.proyecto " + 
				"  and ep.actividad = v.actividad " + 
				"  and ep.obra = v.obra " + 
				") left outer join ( " + 
				"  select t2.ejercicio, month(t1.fec_aprobado) mes, t2.entidad, t2.unidad_ejecutora, t2.programa, t2.subprograma, t2.proyecto, t2.actividad, t2.obra, t2.renglon, sum(t2.monto_renglon) gasto_total  " + 
				"				from sicoinprod.eg_gastos_hoja t1, sicoinprod.eg_gastos_detalle t2   " + 
				"				where t1.ejercicio = t2.ejercicio    " + 
				"				and t1.entidad = t2.entidad   " + 
				"				and t1.unidad_ejecutora = t2.unidad_ejecutora   " + 
				"				and t1.no_cur = t2.no_cur     " + 
				"				and t1.clase_registro IN ('DEV', 'CYD', 'RDP', 'REG')   " + 
				"				and t1.estado = 'APROBADO'   " + 
				"				and t2.ejercicio = ?  " + 
				"				group by t2.ejercicio, month(t1.fec_aprobado), t2.entidad, t2.unidad_ejecutora, t2.programa, t2.subprograma, t2.proyecto, t2.actividad, t2.obra, t2.renglon " + 
				") ejecutado on ( " + 
				"  ejecutado.ejercicio = ep.ejercicio " + 
				"  and ejecutado.mes = t.mes " + 
				"  and ejecutado.entidad = ep.entidad " + 
				"  and ejecutado.unidad_ejecutora = ep.unidad_ejecutora " + 
				"  and ejecutado.programa = ep.programa " + 
				"  and ejecutado.subprograma = ep.subprograma " + 
				"  and ejecutado.proyecto = ep.proyecto " + 
				"  and ejecutado.actividad = ep.actividad " + 
				"  and ejecutado.obra = ep.obra " + 
				"  and ejecutado.renglon = v.renglon " + 
				") left outer join( " + 
				"  select d.ejercicio,month(h.fecha_aprobado) mes, sum(d.cantidad_unidades) meta_cantidad_unidades_avance, d.entidad, d.unidad_ejecutora, d.programa, d.subprograma, d.proyecto, d.actividad, d.obra, d.codigo_meta " + 
				"            from sicoinprod.sf_ejecucion_hoja_4 h, sicoinprod.sf_ejecucion_detalle_4 d " + 
				"            where h.ejercicio = ? " + 
				"            and h.ejercicio=d.ejercicio " + 
				"            and h.entidad = d.entidad " + 
				"            and h.unidad_ejecutora = d.unidad_ejecutora " + 
				"            and h.no_cur = d.no_cur " + 
				"            and h.estado = 'APROBADO'  " + 
				"            group by d.ejercicio,month(h.fecha_aprobado), d.entidad, d.unidad_ejecutora, d.programa, d.subprograma, d.proyecto, d.actividad, d.obra, d.codigo_meta " + 
				"union all " + 
				"select d.ejercicio,month(h.fecha_aprobado), sum(d.cantidad_unidades), d.entidad, 0, d.programa, d.subprograma, d.proyecto, d.actividad, d.obra, d.codigo_meta " + 
				"            from sicoinprod.sf_ejecucion_hoja_4 h, sicoinprod.sf_ejecucion_detalle_4 d " + 
				"            where h.ejercicio = ? " + 
				"            and h.ejercicio=d.ejercicio " + 
				"            and h.entidad = d.entidad " + 
				"            and h.unidad_ejecutora = d.unidad_ejecutora " + 
				"            and h.no_cur = d.no_cur " + 
				"            and h.estado = 'APROBADO' " + 
				"            and h.unidad_ejecutora>0  " + 
				"            group by d.ejercicio,month(h.fecha_aprobado), d.entidad, d.programa, d.subprograma, d.proyecto, d.actividad, d.obra, d.codigo_meta " + 
				") ef on ( " + 
				"  ef.ejercicio = ep.ejercicio " + 
				"  and ef.entidad = ep.entidad " + 
				"  and ef.unidad_ejecutora = ep.unidad_ejecutora " + 
				"  and ef.programa = ep.programa " + 
				"  and ef.subprograma = ep.subprograma " + 
				"  and ef.proyecto = ep.proyecto " + 
				"  and ef.actividad = ep.actividad " + 
				"  and ef.obra = ep.obra " + 
				"  and ef.codigo_meta = m.codigo_meta " + 
				"  and ef.mes = t.mes " + 
				"), dashboard.meta_presidencial mp, " + 
				"sicoinprod.fp_unidad_medida um, " + 
				"sicoinprod.cg_entidades em, " + 
				"sicoinprod.cg_entidades uem, " + 
				"sicoinprod.cp_estructuras pm, " + 
				"sicoinprod.cp_estructuras spm, " + 
				"sicoinprod.cp_estructuras prm, " + 
				"sicoinprod.cp_estructuras ao, " +
				"sicoinprod.cp_objetos_gasto rm "+
				"where t.ejercicio = ? " + 
				"and t.dia = 1 " + 
				"and ep.ejercicio = t.ejercicio " + 
				"and mp.id = ep.meta_presidencialid " + 
				"and mm.entidad = ep.entidad " + 
				"and mm.unidad_ejecutora = ep.unidad_ejecutora " + 
				"and mm.programa = ep.programa " + 
				"and mm.subprograma = ep.subprograma " + 
				"and mm.proyecto = ep.proyecto " + 
				"and mm.actividad = ep.actividad " + 
				"and mm.obra = ep.obra " + 
				"and mm.ejercicio = ep.ejercicio " + 
				"and m.unidad_medida = um.codigo " + 
				"and m.ejercicio = um.ejercicio " + 
				"and m.entidad = ep.entidad " + 
				"and m.unidad_ejecutora = ep.unidad_ejecutora " + 
				"and m.programa = ep.programa " + 
				"and m.subprograma = ep.subprograma " + 
				"and m.proyecto = ep.proyecto " + 
				"and m.actividad = ep.actividad " + 
				"and m.obra = ep.obra " + 
				"and m.codigo_meta = mm.codigo_meta " + 
				"and em.ejercicio = t.ejercicio " + 
				"and em.entidad = ep.entidad " + 
				"and em.unidad_ejecutora = 0 " + 
				"and uem.ejercicio = t.ejercicio " + 
				"and uem.entidad = ep.entidad " + 
				"and uem.unidad_ejecutora = ep.unidad_ejecutora " + 
				"and pm.ejercicio = t.ejercicio " + 
				"and pm.entidad=ep.entidad " + 
				"and pm.unidad_ejecutora=ep.unidad_ejecutora " + 
				"and pm.programa = ep.programa " + 
				"and pm.nivel_estructura = 2 " + 
				"and spm.ejercicio = t.ejercicio " + 
				"and spm.entidad=ep.entidad " + 
				"and spm.unidad_ejecutora=ep.unidad_ejecutora " + 
				"and spm.programa = ep.programa " + 
				"and spm.subprograma = ep.subprograma " + 
				"and spm.nivel_estructura = 3 " + 
				"and prm.ejercicio = t.ejercicio " + 
				"and prm.entidad=ep.entidad " + 
				"and prm.unidad_ejecutora=ep.unidad_ejecutora " + 
				"and prm.programa = ep.programa " + 
				"and prm.subprograma = ep.subprograma " + 
				"and prm.proyecto = ep.proyecto " + 
				"and prm.nivel_estructura = 4 " + 
				"and ao.ejercicio = t.ejercicio " + 
				"and ao.entidad=ep.entidad " + 
				"and ao.unidad_ejecutora=ep.unidad_ejecutora " + 
				"and ao.programa = ep.programa " + 
				"and ao.subprograma = ep.subprograma " + 
				"and ao.proyecto = ep.proyecto " + 
				"and ((ao.actividad = ep.actividad and ao.obra = 0) or (ao.actividad = 0 and ao.obra = ep.obra)) " + 
				"and ao.nivel_estructura = 5 "+
				"and rm.renglon = v.renglon "+
				"and rm.ejercicio = t.ejercicio ";
		try{
			if(!conn.isClosed() && CMemSQL.connect()){
				PreparedStatement pstm = conn.prepareStatement(sql);
				pstm.setInt(1, now.getYear());
				pstm.setInt(2, now.getYear());
				pstm.setInt(3, now.getYear());
				pstm.setInt(4, now.getYear());
				pstm.setInt(5, now.getYear());
				ResultSet rs = pstm.executeQuery();
				PreparedStatement pstmi = CMemSQL.getConnection().prepareStatement("INSERT INTO mv_meta_presidencial(ejercicio, mes, id, nombre, entidad, entidad_nombre, "
						+ "unidad_ejecutora, unidad_ejecutora_nombre, programa, programa_nombre, subprograma, subprograma_nombre, proyecto, proyecto_nombre, actividad, obra, "
						+ "actividad_obra_nombre, renglon, renglon_nombre, asignado, gasto_total, codigo_meta, meta_cantidad, meta_adicion, meta_disminucion, meta_descripcion, "
						+ "meta_medida, meta_cantidad_unidades_avance, vigente_1, vigente_2, vigente_3, vigente_4, vigente_5, vigente_6, vigente_7, vigente_8, vigente_9, "
						+ "vigente_10, vigente_11, vigente_12) VALUES (?, ?, ?, ?, ?, ?, ?, ?, "
						+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
				long rows=0;
				while(rs.next()){
					if(rows==0)
						deleteMv_meta_presidencial(CMemSQL.getConnection(),ejercicio);
					pstmi.setInt(1, rs.getInt("ejercicio"));
					pstmi.setInt(2, rs.getInt("mes"));
					pstmi.setInt(3, rs.getInt("id"));
					pstmi.setString(4, rs.getString("nombre"));
					pstmi.setInt(5, rs.getInt("entidad"));
					pstmi.setString(6, rs.getString("nombre_entidad"));
					pstmi.setInt(7, rs.getInt("unidad_ejecutora"));
					pstmi.setString(8, rs.getString("nombre_unidad_ejecutora"));
					pstmi.setInt(9, rs.getInt("programa"));
					pstmi.setString(10, rs.getString("nombre_programa"));
					pstmi.setInt(11, rs.getInt("subprograma"));
					pstmi.setString(12, rs.getString("nombre_subprograma"));
					pstmi.setInt(13, rs.getInt("proyecto"));
					pstmi.setString(14, rs.getString("nombre_proyecto"));
					pstmi.setInt(15, rs.getInt("actividad"));
					pstmi.setInt(16, rs.getInt("obra"));
					pstmi.setString(17, rs.getString("nombre_actividad_obra"));
					pstmi.setInt(18, rs.getInt("renglon"));
					pstmi.setString(19, rs.getString("nombre_renglon"));
					pstmi.setDouble(20, rs.getDouble("asignado"));
					pstmi.setDouble(21, rs.getDouble("gasto_total"));
					pstmi.setInt(22, rs.getInt("codigo_meta"));
					pstmi.setLong(23, rs.getLong("meta_cantidad"));
					pstmi.setLong(24, rs.getLong("meta_adicion"));
					pstmi.setLong(25, rs.getLong("meta_disminucion"));
					pstmi.setString(26, rs.getString("meta_descripcion"));
					pstmi.setString(27, rs.getString("meta_medida"));
					pstmi.setLong(28, rs.getLong("meta_cantidad_unidades_avance"));
					pstmi.setDouble(29, rs.getDouble("vigente_1"));
					pstmi.setDouble(30, rs.getDouble("vigente_2"));
					pstmi.setDouble(31, rs.getDouble("vigente_3"));
					pstmi.setDouble(32, rs.getDouble("vigente_4"));
					pstmi.setDouble(33, rs.getDouble("vigente_5"));
					pstmi.setDouble(34, rs.getDouble("vigente_6"));
					pstmi.setDouble(35, rs.getDouble("vigente_7"));
					pstmi.setDouble(36, rs.getDouble("vigente_8"));
					pstmi.setDouble(37, rs.getDouble("vigente_9"));
					pstmi.setDouble(38, rs.getDouble("vigente_10"));
					pstmi.setDouble(39, rs.getDouble("vigente_11"));
					pstmi.setDouble(40, rs.getDouble("vigente_12"));
					pstmi.addBatch();
					rows++;
					if(rows%1000 == 0){
						pstmi.executeBatch();
						CLogger.writeConsole(String.join(" ", "Records escritos: ", String.valueOf(rows)));
					}
				}
			}
		}
		catch(Exception e){
			CLogger.writeFullConsole("CMetas.class 1:", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
	
	public static boolean calcularMv_meta_presidencial_descentralizadas(Connection conn, int ejercicio){
		boolean ret = false;
		DateTime now = new DateTime();
		String sql="select t.ejercicio, t.mes, mp.id, mp.nombre, em.nombre nombre_entidad,uem.nombre nombre_unidad_ejecutora, pm.nom_estructura nombre_programa, spm.nom_estructura nombre_subprograma, prm.nom_estructura nombre_proyecto, ao.nom_estructura nombre_actividad_obra, " + 
				"         ep.entidad, ep.unidad_ejecutora, ep.programa, ep.subprograma, ep.proyecto, ep.actividad, ep.obra, mm.codigo_meta,  " + 
				"         v.renglon, rm.nombre nombre_renglon, v.asignado, v.vigente_1, v.vigente_2, v.vigente_3, v.vigente_4, v.vigente_5, v.vigente_6, v.vigente_7, v.vigente_8, v.vigente_9, v.vigente_10, v.vigente_11, v.vigente_12, " + 
				"         ejecutado.gasto_total, m.cantidad meta_cantidad, m.adicion meta_adicion, m.disminucion meta_disminucion, m.descripcion meta_descripcion, um.nombre meta_medida, ef.meta_cantidad_unidades_avance " + 
				"from dashboard.tiempo t, " + 
				"dashboard.meta_presidencial_ep_meta mm, sicoindes.sf_meta m, " + 
				"dashboard.meta_presidencial_ep ep left outer join ( " + 
				" " + 
				"select p.ejercicio,p.entidad, p.unidad_ejecutora, p.renglon, p.programa, p.subprograma, p.proyecto, p.actividad, p.obra, sum(p.asignado) asignado,(sum(p.asignado) + sum(p.adicion) + sum(p.disminucion) + sum(p.traspaso_p)+ sum(p.traspaso_n)+ sum(p.transferencia_p) + sum(p.transferencia_n)) vigente_actual,  " + 
				"				sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) vigente_1,  " + 
				"				sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) +  " + 
				"						   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) vigente_2,   " + 
				"				sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) +  " + 
				"						   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) +   " + 
				"						   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) vigente_3,   " + 
				"				sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) +   " + 
				"						   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) +   " + 
				"						   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) +   " + 
				"						   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) vigente_4,   " + 
				"				sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) +   " + 
				"						   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) +   " + 
				"			   		   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) +   " + 
				"						   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) +   " + 
				"					   	   (sum(p.adicion_05) + sum(p.disminucion_05) + sum(p.traspaso_p05)+ sum(p.traspaso_n05)+ sum(p.transferencia_p05) + sum(p.transferencia_n05) ) vigente_5,   " + 
				"				sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum( " + 
				"				p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) +   " + 
				"						   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) +   " + 
				"						   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) +   " + 
				"						   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) +   " + 
				"					   	   (sum(p.adicion_05) + sum(p.disminucion_05) + sum(p.traspaso_p05)+ sum(p.traspaso_n05)+ sum(p.transferencia_p05) + sum(p.transferencia_n05) ) +   " + 
				"						   (sum(p.adicion_06) + sum(p.disminucion_06) + sum(p.traspaso_p06)+ sum(p.traspaso_n06)+ sum(p.transferencia_p06) + sum(p.transferencia_n06) ) vigente_6,   " + 
				"				sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) +   " + 
				"						   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) +   " + 
				"						   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) +   " + 
				"						   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) +   " + 
				"					   	   (sum(p.adicion_05) + sum(p.disminucion_05) + sum(p.traspaso_p05)+ sum(p.traspaso_n05)+ sum(p.transferencia_p05) + sum(p.transferencia_n05) ) +   " + 
				"						   (sum(p.adicion_06) + sum(p.disminucion_06) + sum(p.traspaso_p06)+ sum(p.traspaso_n06)+ sum(p.transferencia_p06) + sum(p.transferencia_n06) ) +   " + 
				"						   (sum(p.adicion_07) + sum(p.disminucion_07) + sum(p.traspaso_p07)+ sum(p.traspaso_n07)+ sum(p.transferencia_p07) + sum(p.transferencia_n07) ) vigente_7,   " + 
				"				sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) +   " + 
				"						   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) +   " + 
				"						   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) +   " + 
				"						   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) +   " + 
				"					   	   (sum(p.adicion_05) + sum(p.disminucion_05) + sum(p.traspaso_p05)+ sum(p.traspaso_n05)+ sum(p.transferencia_p05) + sum(p.transferencia_n05) ) +   " + 
				"						   (sum(p.adicion_06) + sum(p.disminucion_06) + sum(p.traspaso_p06)+ sum(p.traspaso_n06)+ sum(p.transferencia_p06) + sum(p.transferencia_n06) ) +   " + 
				"						   (sum(p.adicion_07) + sum(p.disminucion_07) + sum(p.traspaso_p07)+ sum(p.traspaso_n07)+ sum(p.transferencia_p07) + sum(p.transferencia_n07) ) +   " + 
				"						   (sum(p.adicion_08) + sum(p.disminucion_08) + sum(p.traspaso_p08)+ sum(p.traspaso_n08)+ sum(p.transferencia_p08) + sum(p.transferencia_n08) ) vigente_8,   " + 
				"				sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) +   " + 
				"						   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) +   " + 
				"						   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) +   " + 
				"						   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) +   " + 
				"					   	   (sum(p.adicion_05) + sum(p.disminucion_05) + sum(p.traspaso_p05)+ sum(p.traspaso_n05)+ sum(p.transferencia_p05) + sum(p.transferencia_n05) ) +   " + 
				"						   (sum(p.adicion_06) + sum(p.disminucion_06) + sum(p.traspaso_p06)+ sum(p.traspaso_n06)+ sum(p.transferencia_p06) + sum(p.transferencia_n06) ) +   " + 
				"						   (sum(p.adicion_07) + sum(p.disminucion_07) + sum(p.traspaso_p07)+ sum(p.traspaso_n07)+ sum(p.transferencia_p07) + sum(p.transferencia_n07) ) +   " + 
				"						   (sum(p.adicion_08) + sum(p.disminucion_08) + sum(p.traspaso_p08)+ sum(p.traspaso_n08)+ sum(p.transferencia_p08) + sum(p.transferencia_n08) ) +   " + 
				"						   (sum(p.adicion_09) + sum(p.disminucion_09) + sum(p.traspaso_p09)+ sum(p.traspaso_n09)+ sum(p.transferencia_p09) + sum(p.transferencia_n09) ) vigente_9,   " + 
				"				sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) +   " + 
				"						   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) +   " + 
				"						   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) +   " + 
				"						   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) +   " + 
				"					   	   (sum(p.adicion_05) + sum(p.disminucion_05) + sum(p.traspaso_p05)+ sum(p.traspaso_n05)+ sum(p.transferencia_p05) + sum(p.transferencia_n05) ) +   " + 
				"						   (sum(p.adicion_06) + sum(p.disminucion_06) + sum(p.traspaso_p06)+ sum(p.traspaso_n06)+ sum(p.transferencia_p06) + sum(p.transferencia_n06) ) +   " + 
				"						   (sum(p.adicion_07) + sum(p.disminucion_07) + sum(p.traspaso_p07)+ sum(p.traspaso_n07)+ sum(p.transferencia_p07) + sum(p.transferencia_n07) ) +   " + 
				"						   (sum(p.adicion_08) + sum(p.disminucion_08) + sum(p.traspaso_p08)+ sum(p.traspaso_n08)+ sum(p.transferencia_p08) + sum(p.transferencia_n08) ) +   " + 
				"						   (sum(p.adicion_09) + sum(p.disminucion_09) + sum(p.traspaso_p09)+ sum(p.traspaso_n09)+ sum(p.transferencia_p09) + sum(p.transferencia_n09) ) +   " + 
				"						   (sum(p.adicion_10) + sum(p.disminucion_10) + sum(p.traspaso_p10)+ sum(p.traspaso_n10)+ sum(p.transferencia_p10) + sum(p.transferencia_n10) ) vigente_10,   " + 
				"				sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) +   " + 
				"						   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) +   " + 
				"						   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) +   " + 
				"						   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) +   " + 
				"					   	   (sum(p.adicion_05) + sum(p.disminucion_05) + sum(p.traspaso_p05)+ sum(p.traspaso_n05)+ sum(p.transferencia_p05) + sum(p.transferencia_n05) ) +   " + 
				"						   (sum(p.adicion_06) + sum(p.disminucion_06) + sum(p.traspaso_p06)+ sum(p.traspaso_n06)+ sum(p.transferencia_p06) + sum(p.transferencia_n06) ) +   " + 
				"						   (sum(p.adicion_07) + sum(p.disminucion_07) + sum(p.traspaso_p07)+ sum(p.traspaso_n07)+ sum(p.transferencia_p07) + sum(p.transferencia_n07) ) +   " + 
				"						   (sum(p.adicion_08) + sum(p.disminucion_08) + sum(p.traspaso_p08)+ sum(p.traspaso_n08)+ sum(p.transferencia_p08) + sum(p.transferencia_n08) ) +   " + 
				"						   (sum(p.adicion_09) + sum(p.disminucion_09) + sum(p.traspaso_p09)+ sum(p.traspaso_n09)+ sum(p.transferencia_p09) + sum(p.transferencia_n09) ) +   " + 
				"						   (sum(p.adicion_10) + sum(p.disminucion_10) + sum(p.traspaso_p10)+ sum(p.traspaso_n10)+ sum(p.transferencia_p10) + sum(p.transferencia_n10) ) +   " + 
				"			  		   (sum(p.adicion_11) + sum(p.disminucion_11) + sum(p.traspaso_p11)+ sum(p.traspaso_n11)+ sum(p.transferencia_p11) + sum(p.transferencia_n11) ) vigente_11,   " + 
				"				sum(p.asignado) + (sum(p.adicion_01) + sum(p.disminucion_01) + sum(p.traspaso_p01)+ sum(p.traspaso_n01)+ sum(p.transferencia_p01) + sum(p.transferencia_n01) ) +   " + 
				"						   (sum(p.adicion_02) + sum(p.disminucion_02) + sum(p.traspaso_p02)+ sum(p.traspaso_n02)+ sum(p.transferencia_p02) + sum(p.transferencia_n02) ) +   " + 
				"						   (sum(p.adicion_03) + sum(p.disminucion_03) + sum(p.traspaso_p03)+ sum(p.traspaso_n03)+ sum(p.transferencia_p03) + sum(p.transferencia_n03) ) +   " + 
				"						   (sum(p.adicion_04) + sum(p.disminucion_04) + sum(p.traspaso_p04)+ sum(p.traspaso_n04)+ sum(p.transferencia_p04) + sum(p.transferencia_n04) ) +   " + 
				"					   	   (sum(p.adicion_05) + sum(p.disminucion_05) + sum(p.traspaso_p05)+ sum(p.traspaso_n05)+ sum(p.transferencia_p05) + sum(p.transferencia_n05) ) +   " + 
				"						   (sum(p.adicion_06) + sum(p.disminucion_06) + sum(p.traspaso_p06)+ sum(p.traspaso_n06)+ sum(p.transferencia_p06) + sum(p.transferencia_n06) ) +   " + 
				"						   (sum(p.adicion_07) + sum(p.disminucion_07) + sum(p.traspaso_p07)+ sum(p.traspaso_n07)+ sum(p.transferencia_p07) + sum(p.transferencia_n07) ) +   " + 
				"						   (sum(p.adicion_08) + sum(p.disminucion_08) + sum(p.traspaso_p08)+ sum(p.traspaso_n08)+ sum(p.transferencia_p08) + sum(p.transferencia_n08) ) +   " + 
				"						   (sum(p.adicion_09) + sum(p.disminucion_09) + sum(p.traspaso_p09)+ sum(p.traspaso_n09)+ sum(p.transferencia_p09) + sum(p.transferencia_n09) ) +   " + 
				"						   (sum(p.adicion_10) + sum(p.disminucion_10) + sum(p.traspaso_p10)+ sum(p.traspaso_n10)+ sum(p.transferencia_p10) + sum(p.transferencia_n10) ) +   " + 
				"						   (sum(p.adicion_11) + sum(p.disminucion_11) + sum(p.traspaso_p11)+ sum(p.traspaso_n11)+ sum(p.transferencia_p11) + sum(p.transferencia_n11) ) +   " + 
				"						   (sum(p.adicion_12) + sum(p.disminucion_12) + sum(p.traspaso_p12)+ sum(p.traspaso_n12)+ sum(p.transferencia_p12) + sum(p.transferencia_n12) ) vigente_12   " + 
				"				from sicoindes.EG_F6_PARTIDAS p  " + 
				"				where p.ejercicio= ?  " + 
				"				group by p.ejercicio,p.entidad,p.unidad_ejecutora, p.programa,p.subprograma, p.proyecto, p.actividad, p.obra, p.renglon " + 
				") v " + 
				"on ( " + 
				"  ep.ejercicio = v.ejercicio " + 
				"  and ep.entidad = v.entidad " + 
				"  and ep.unidad_ejecutora = v.unidad_ejecutora " + 
				"  and ep.programa = v.programa " + 
				"  and ep.subprograma = v.subprograma " + 
				"  and ep.proyecto = v.proyecto " + 
				"  and ep.actividad = v.actividad " + 
				"  and ep.obra = v.obra " + 
				") left outer join ( " + 
				"  select t2.ejercicio, month(t1.fec_aprobado) mes, t2.entidad, t2.unidad_ejecutora, t2.programa, t2.subprograma, t2.proyecto, t2.actividad, t2.obra, t2.renglon, sum(t2.monto_renglon) gasto_total  " + 
				"				from sicoindes.eg_gastos_hoja t1, sicoindes.eg_gastos_detalle t2   " + 
				"				where t1.ejercicio = t2.ejercicio    " + 
				"				and t1.entidad = t2.entidad   " + 
				"				and t1.unidad_ejecutora = t2.unidad_ejecutora   " + 
				"				and t1.no_cur = t2.no_cur     " + 
				"				and t1.clase_registro IN ('DEV', 'CYD', 'RDP', 'REG')   " + 
				"				and t1.estado = 'APROBADO'   " + 
				"				and t2.ejercicio = ?  " + 
				"				group by t2.ejercicio, month(t1.fec_aprobado), t2.entidad, t2.unidad_ejecutora, t2.programa, t2.subprograma, t2.proyecto, t2.actividad, t2.obra, t2.renglon " + 
				") ejecutado on ( " + 
				"  ejecutado.ejercicio = ep.ejercicio " + 
				"  and ejecutado.mes = t.mes " + 
				"  and ejecutado.entidad = ep.entidad " + 
				"  and ejecutado.unidad_ejecutora = ep.unidad_ejecutora " + 
				"  and ejecutado.programa = ep.programa " + 
				"  and ejecutado.subprograma = ep.subprograma " + 
				"  and ejecutado.proyecto = ep.proyecto " + 
				"  and ejecutado.actividad = ep.actividad " + 
				"  and ejecutado.obra = ep.obra " + 
				"  and ejecutado.renglon = v.renglon " + 
				") left outer join( " + 
				"  select d.ejercicio,month(h.fecha_aprobado) mes, sum(d.cantidad_unidades) meta_cantidad_unidades_avance, d.entidad, d.unidad_ejecutora, d.programa, d.subprograma, d.proyecto, d.actividad, d.obra, d.codigo_meta " + 
				"            from sicoindes.sf_ejecucion_hoja_4 h, sicoindes.sf_ejecucion_detalle_4 d " + 
				"            where h.ejercicio = ? " + 
				"            and h.ejercicio=d.ejercicio " + 
				"            and h.entidad = d.entidad " + 
				"            and h.unidad_ejecutora = d.unidad_ejecutora " + 
				"            and h.no_cur = d.no_cur " + 
				"            and h.estado = 'APROBADO'  " + 
				"            group by d.ejercicio,month(h.fecha_aprobado), d.entidad, d.unidad_ejecutora, d.programa, d.subprograma, d.proyecto, d.actividad, d.obra, d.codigo_meta " + 
				"union all " + 
				"select d.ejercicio,month(h.fecha_aprobado), sum(d.cantidad_unidades), d.entidad, 0, d.programa, d.subprograma, d.proyecto, d.actividad, d.obra, d.codigo_meta " + 
				"            from sicoindes.sf_ejecucion_hoja_4 h, sicoindes.sf_ejecucion_detalle_4 d " + 
				"            where h.ejercicio = ? " + 
				"            and h.ejercicio=d.ejercicio " + 
				"            and h.entidad = d.entidad " + 
				"            and h.unidad_ejecutora = d.unidad_ejecutora " + 
				"            and h.no_cur = d.no_cur " + 
				"            and h.estado = 'APROBADO' " + 
				"            and h.unidad_ejecutora>0  " + 
				"            group by d.ejercicio,month(h.fecha_aprobado), d.entidad, d.programa, d.subprograma, d.proyecto, d.actividad, d.obra, d.codigo_meta " + 
				") ef on ( " + 
				"  ef.ejercicio = ep.ejercicio " + 
				"  and ef.entidad = ep.entidad " + 
				"  and ef.unidad_ejecutora = ep.unidad_ejecutora " + 
				"  and ef.programa = ep.programa " + 
				"  and ef.subprograma = ep.subprograma " + 
				"  and ef.proyecto = ep.proyecto " + 
				"  and ef.actividad = ep.actividad " + 
				"  and ef.obra = ep.obra " + 
				"  and ef.codigo_meta = m.codigo_meta " + 
				"  and ef.mes = t.mes " + 
				"), dashboard.meta_presidencial mp, " + 
				"sicoindes.fp_unidad_medida um, " + 
				"sicoindes.cg_entidades em, " + 
				"sicoindes.cg_entidades uem, " + 
				"sicoindes.cp_estructuras pm, " + 
				"sicoindes.cp_estructuras spm, " + 
				"sicoindes.cp_estructuras prm, " + 
				"sicoindes.cp_estructuras ao, " +
				"sicoindes.cp_objetos_gasto rm "+
				"where t.ejercicio = ? " + 
				"and t.dia = 1 " + 
				"and ep.ejercicio = t.ejercicio " + 
				"and mp.id = ep.meta_presidencialid " + 
				"and mm.entidad = ep.entidad " + 
				"and mm.unidad_ejecutora = ep.unidad_ejecutora " + 
				"and mm.programa = ep.programa " + 
				"and mm.subprograma = ep.subprograma " + 
				"and mm.proyecto = ep.proyecto " + 
				"and mm.actividad = ep.actividad " + 
				"and mm.obra = ep.obra " + 
				"and mm.ejercicio = ep.ejercicio " + 
				"and m.unidad_medida = um.codigo " + 
				"and m.ejercicio = um.ejercicio " + 
				"and m.entidad = ep.entidad " + 
				"and m.unidad_ejecutora = ep.unidad_ejecutora " + 
				"and m.programa = ep.programa " + 
				"and m.subprograma = ep.subprograma " + 
				"and m.proyecto = ep.proyecto " + 
				"and m.actividad = ep.actividad " + 
				"and m.obra = ep.obra " + 
				"and m.codigo_meta = mm.codigo_meta " + 
				"and em.ejercicio = t.ejercicio " + 
				"and em.entidad = ep.entidad " + 
				"and em.unidad_ejecutora = 0 " + 
				"and uem.ejercicio = t.ejercicio " + 
				"and uem.entidad = ep.entidad " + 
				"and uem.unidad_ejecutora = ep.unidad_ejecutora " + 
				"and pm.ejercicio = t.ejercicio " + 
				"and pm.entidad=ep.entidad " + 
				"and pm.unidad_ejecutora=ep.unidad_ejecutora " + 
				"and pm.programa = ep.programa " + 
				"and pm.nivel_estructura = 2 " + 
				"and spm.ejercicio = t.ejercicio " + 
				"and spm.entidad=ep.entidad " + 
				"and spm.unidad_ejecutora=ep.unidad_ejecutora " + 
				"and spm.programa = ep.programa " + 
				"and spm.subprograma = ep.subprograma " + 
				"and spm.nivel_estructura = 3 " + 
				"and prm.ejercicio = t.ejercicio " + 
				"and prm.entidad=ep.entidad " + 
				"and prm.unidad_ejecutora=ep.unidad_ejecutora " + 
				"and prm.programa = ep.programa " + 
				"and prm.subprograma = ep.subprograma " + 
				"and prm.proyecto = ep.proyecto " + 
				"and prm.nivel_estructura = 4 " + 
				"and ao.ejercicio = t.ejercicio " + 
				"and ao.entidad=ep.entidad " + 
				"and ao.unidad_ejecutora=ep.unidad_ejecutora " + 
				"and ao.programa = ep.programa " + 
				"and ao.subprograma = ep.subprograma " + 
				"and ao.proyecto = ep.proyecto " + 
				"and ((ao.actividad = ep.actividad and ao.obra = 0) or (ao.actividad = 0 and ao.obra = ep.obra)) " + 
				"and ao.nivel_estructura = 5 "+
				"and rm.renglon = v.renglon "+
				"and rm.ejercicio = t.ejercicio ";
		try{
			if(!conn.isClosed() && CMemSQL.connect()){
				PreparedStatement pstm = conn.prepareStatement(sql);
				pstm.setInt(1, now.getYear());
				pstm.setInt(2, now.getYear());
				pstm.setInt(3, now.getYear());
				pstm.setInt(4, now.getYear());
				pstm.setInt(5, now.getYear());
				ResultSet rs = pstm.executeQuery();
				PreparedStatement pstmi = CMemSQL.getConnection().prepareStatement("INSERT INTO mv_meta_presidencial(ejercicio, mes, id, nombre, entidad, entidad_nombre, "
						+ "unidad_ejecutora, unidad_ejecutora_nombre, programa, programa_nombre, subprograma, subprograma_nombre, proyecto, proyecto_nombre, actividad, obra, "
						+ "actividad_obra_nombre, renglon, renglon_nombre, asignado, gasto_total, codigo_meta, meta_cantidad, meta_adicion, meta_disminucion, meta_descripcion, "
						+ "meta_medida, meta_cantidad_unidades_avance, vigente_1, vigente_2, vigente_3, vigente_4, vigente_5, vigente_6, vigente_7, vigente_8, vigente_9, "
						+ "vigente_10, vigente_11, vigente_12) VALUES (?, ?, ?, ?, ?, ?, ?, ?, "
						+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
				long rows=0;
				while(rs.next()){
					pstmi.setInt(1, rs.getInt("ejercicio"));
					pstmi.setInt(2, rs.getInt("mes"));
					pstmi.setInt(3, rs.getInt("id"));
					pstmi.setString(4, rs.getString("nombre"));
					pstmi.setInt(5, rs.getInt("entidad"));
					pstmi.setString(6, rs.getString("nombre_entidad"));
					pstmi.setInt(7, rs.getInt("unidad_ejecutora"));
					pstmi.setString(8, rs.getString("nombre_unidad_ejecutora"));
					pstmi.setInt(9, rs.getInt("programa"));
					pstmi.setString(10, rs.getString("nombre_programa"));
					pstmi.setInt(11, rs.getInt("subprograma"));
					pstmi.setString(12, rs.getString("nombre_subprograma"));
					pstmi.setInt(13, rs.getInt("proyecto"));
					pstmi.setString(14, rs.getString("nombre_proyecto"));
					pstmi.setInt(15, rs.getInt("actividad"));
					pstmi.setInt(16, rs.getInt("obra"));
					pstmi.setString(17, rs.getString("nombre_actividad_obra"));
					pstmi.setInt(18, rs.getInt("renglon"));
					pstmi.setString(19, rs.getString("nombre_renglon"));
					pstmi.setDouble(20, rs.getDouble("asignado"));
					pstmi.setDouble(21, rs.getDouble("gasto_total"));
					pstmi.setInt(22, rs.getInt("codigo_meta"));
					pstmi.setLong(23, rs.getLong("meta_cantidad"));
					pstmi.setLong(24, rs.getLong("meta_adicion"));
					pstmi.setLong(25, rs.getLong("meta_disminucion"));
					pstmi.setString(26, rs.getString("meta_descripcion"));
					pstmi.setString(27, rs.getString("meta_medida"));
					pstmi.setLong(28, rs.getLong("meta_cantidad_unidades_avance"));
					pstmi.setDouble(29, rs.getDouble("vigente_1"));
					pstmi.setDouble(30, rs.getDouble("vigente_2"));
					pstmi.setDouble(31, rs.getDouble("vigente_3"));
					pstmi.setDouble(32, rs.getDouble("vigente_4"));
					pstmi.setDouble(33, rs.getDouble("vigente_5"));
					pstmi.setDouble(34, rs.getDouble("vigente_6"));
					pstmi.setDouble(35, rs.getDouble("vigente_7"));
					pstmi.setDouble(36, rs.getDouble("vigente_8"));
					pstmi.setDouble(37, rs.getDouble("vigente_9"));
					pstmi.setDouble(38, rs.getDouble("vigente_10"));
					pstmi.setDouble(39, rs.getDouble("vigente_11"));
					pstmi.setDouble(40, rs.getDouble("vigente_12"));
					pstmi.addBatch();
					rows++;
					if(rows%1000 == 0){
						pstmi.executeBatch();
						CLogger.writeConsole(String.join(" ", "Records escritos: ", String.valueOf(rows)));
					}
				}
			}
		}
		catch(Exception e){
			CLogger.writeFullConsole("CMetas.class 1:", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
	
	public static boolean deleteMv_meta_presidencial(Connection conn,int ejercicio) {
		boolean ret = false;
		try{
			if(!conn.isClosed()){
				PreparedStatement pstm = conn.prepareStatement("DELETE FROM mv_meta_presidencial WHERE ejercicio = ?");
				pstm.setInt(1, ejercicio);
				if(pstm.executeUpdate()>0)
					ret = true;
			}
		}
		catch(Exception e){
			CLogger.writeFullConsole("CMetas.class 2:", e);
		}
		return ret;
	}
	
}
