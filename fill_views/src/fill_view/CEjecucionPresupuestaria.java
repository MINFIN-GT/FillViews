package fill_view;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import utilities.CLogger;

public class CEjecucionPresupuestaria {
	
	public static boolean loadEjecucionPresupuestaria(Connection conn){
		
		boolean ret = false;
		try{
			if( !conn.isClosed() && CMemSQL.connect()){
				ret = true;

				CLogger.writeConsole("CEjecucionPresupuestaria Entidades:");
				PreparedStatement pstm = conn.prepareStatement("TRUNCATE TABLE dashboard.mv_estructura");
				pstm.executeUpdate();
				pstm.close();
				pstm = conn.prepareStatement("TRUNCATE TABLE dashboard.mv_gasto");
				pstm.executeUpdate();
				pstm.close();
				pstm = conn.prepareStatement("TRUNCATE TABLE dashboard.mv_cuota");
				pstm.executeUpdate();
				pstm.close();
				pstm = conn.prepareStatement("TRUNCATE TABLE dashboard.mv_anticipo");
				pstm.executeUpdate();
				pstm.close();
				pstm = conn.prepareStatement("TRUNCATE TABLE dashboard.mv_vigente");
				pstm.executeUpdate();
				pstm.close();
				pstm = conn.prepareStatement("TRUNCATE TABLE dashboard.mv_ejecucion_presupuestaria");
				pstm.executeUpdate();
				pstm.close(); 
				pstm = conn.prepareStatement("TRUNCATE TABLE dashboard.mv_ejecucion_presupuestaria_geografico");
				pstm.executeUpdate();
				pstm.close();
				
				CLogger.writeConsole("Insertando valores a MV_ESTRUCTURA");
				pstm = conn.prepareStatement("insert into table dashboard.mv_estructura "+
						"select e.ejercicio, e.entidad, e.nombre entidad_nombre, es.sigla, ue.unidad_ejecutora, ue.nombre unidad_ejecutora_nombre,  " + 
						"p.programa, p.nom_estructura programa_nombre,  " + 
						"sp.subprograma, sp.nom_estructura subprograma_nombre,  " + 
						"pr.proyecto, pr.nom_estructura proyecto_nombre,  " + 
						"ao.actividad, ao.obra, ao.nom_estructura actividad_obra_nombre  " + 
						"from sicoinprod.cg_entidades e, sicoinprod.cg_entidades ue " + 
						"left outer join sicoinprod.cp_estructuras p  " + 
						"on( " + 
						"p.ejercicio = ue.ejercicio  " + 
						"and p.entidad = ue.entidad  " + 
						"and p.unidad_ejecutora = ue.unidad_ejecutora  " + 
						"and p.nivel_estructura = 2 " + 
						") left outer join sicoinprod.cp_estructuras sp  " + 
						"on( " + 
						"sp.ejercicio = ue.ejercicio  " + 
						"and sp.entidad = ue.entidad  " + 
						"and sp.unidad_ejecutora = ue.unidad_ejecutora  " + 
						"and sp.programa = p.programa  " + 
						"and sp.nivel_estructura = 3  " + 
						") left outer join sicoinprod.cp_estructuras pr " + 
						"on( " + 
						"pr.ejercicio = ue.ejercicio  " + 
						"and pr.entidad = ue.entidad   " + 
						"and pr.unidad_ejecutora = ue.unidad_ejecutora  " + 
						"and pr.programa = p.programa  " + 
						"and pr.subprograma = sp.subprograma  " + 
						"and pr.nivel_estructura = 4  " + 
						") left outer join sicoinprod.cp_estructuras ao " + 
						"on( " + 
						"ao.ejercicio = ue.ejercicio  " + 
						"and ao.entidad = ue.entidad  " + 
						"and ao.unidad_ejecutora = ue.unidad_ejecutora  " + 
						"and ao.programa = p.programa  " + 
						"and ao.subprograma = sp.subprograma  " + 
						"and ao.proyecto = pr.proyecto  " + 
						"and ao.nivel_estructura = 5 " + 
						"), dashboard.mv_entidad mve, dashboard.entidad_sigla es  " + 
						"where e.ejercicio = year(current_date())  " + 
						"and ue.ejercicio = e.ejercicio  " + 
						"and e.restrictiva = 'N'  " + 
						"and ue.restrictiva = 'N'  " + 
						"and e.unidad_ejecutora = 0  " + 
						"and ue.entidad = e.entidad  " + 
						"and ((e.entidad between 11130000 and 11130020) OR e.entidad = 11140021)   " + 
						"and mve.entidad = e.entidad  " + 
						"and mve.ejercicio = e.ejercicio  " + 
						"and ((ue.unidad_ejecutora = 0 and mve.unidades_ejecutoras=1) or (ue.unidad_ejecutora>0 and mve.unidades_ejecutoras>1))  " + 
						"and es.entidad = e.entidad");
				pstm.executeUpdate();
				pstm.close();
				
				CLogger.writeConsole("Insertando valores a MV_GASTO");
				///Actualiza la vista de gasto
				pstm = conn.prepareStatement("insert into table dashboard.mv_gasto "
						+"select month(gh.fec_aprobado) mes, gd.entidad, gd.unidad_ejecutora, gd.programa, gd.subprograma, gd.proyecto, gd.actividad, gd.obra, gd.renglon, r.nombre renglon_nombre, gd.fuente,     " + 
						"						 						 	gd.renglon - (gd.renglon%100) grupo, gg.nombre grupo_nombre, gd.renglon - (gd.renglon%10) subgrupo, sg.nombre subgrupo_nombre, gd.geografico,      " + 
						"						 						 	sum( case when gh.ejercicio = (year(current_date()) - 5) then gd.monto_renglon else 0 end) ano_1,      " + 
						"						 						 	sum( case when gh.ejercicio = (year(current_date()) - 4) then gd.monto_renglon else 0 end) ano_2,      " + 
						"						 						 	sum( case when gh.ejercicio = (year(current_date()) - 3) then gd.monto_renglon else 0 end) ano_3,      " + 
						"						 						 	sum( case when gh.ejercicio = (year(current_date()) - 2) then gd.monto_renglon else 0 end) ano_4,      " + 
						"						 						 	sum( case when gh.ejercicio = (year(current_date()) - 1) then gd.monto_renglon else 0 end) ano_5,      " + 
						"						 						 	sum( case when gh.ejercicio = (year(current_date())) then gd.monto_renglon else 0 end) ano_actual      " + 
						"						 						 				from sicoinprod.eg_gastos_hoja gh, sicoinprod.eg_gastos_detalle gd,      " + 
						"						 										sicoinprod.cp_grupos_gasto gg, sicoinprod.cp_objetos_gasto sg, sicoinprod.cp_objetos_gasto r  		 " + 
						"						 						 				where gh.ejercicio = gd.ejercicio         " + 
						"						 						 				and gh.entidad = gd.entidad       " + 
						"						 						 				and gh.unidad_ejecutora = gd.unidad_ejecutora       " + 
						"						 						 				and gh.no_cur = gd.no_cur       " + 
						"						 						 				and gh.clase_registro IN ('DEV', 'CYD', 'RDP', 'REG')       " + 
						"						 						 				and gh.estado = 'APROBADO'       " + 
						"						 						 				and gh.ejercicio > (year(current_date())-6)       " + 
						"						  										and gg.ejercicio = year(current_date())   " + 
						"		 				  										and gg.grupo_gasto = (gd.renglon-(gd.renglon%100))   " + 
						"		 				  										and sg.ejercicio = year(current_date())    " + 
						"		 				  										and sg.renglon = (gd.renglon - (gd.renglon%10))        " + 
						"		 				  										and r.ejercicio = year(current_date())   " + 
						"		 				  										and r.renglon = gd.renglon   " + 
						"						 						 				group by month(gh.fec_aprobado), gd.entidad, gd.unidad_ejecutora, gd.programa, gd.subprograma,       " + 
						"						 						 				gd.proyecto, gd.actividad, gd.obra, gg.nombre, sg.nombre, r.nombre, gd.renglon, gd.fuente, gd.geografico  ");
 
				pstm.executeUpdate();
				pstm.close();
				
				CLogger.writeConsole("Insertando valores a MV_CUOTA");
				///Actualiza la vista de cuota
				pstm = conn.prepareStatement("INSERT INTO table dashboard.mv_cuota "+
						"SELECT d.ejercicio, t.mes,  " + 
						"                   d.entidad,  " + 
						"                   d.unidad_ejecutora,  " + 
						"                   d.fuente,   " + 
						"                   sum(case  " + 
						"                   when t.mes = 1 and d.cuatrimestre = 1 then d.cuota_mes1_sol  " + 
						"                   when t.mes = 2 and d.cuatrimestre = 1 then d.cuota_mes2_sol " + 
						"                   when t.mes = 3 and d.cuatrimestre = 1 then d.cuota_mes3_sol " + 
						"                   when t.mes = 4 and d.cuatrimestre = 1 then d.cuota_mes4_sol " + 
						"                   when t.mes = 5 and d.cuatrimestre = 2 then d.cuota_mes1_sol  " + 
						"                   when t.mes = 6 and d.cuatrimestre = 2 then d.cuota_mes2_sol " + 
						"                   when t.mes = 7 and d.cuatrimestre = 2 then d.cuota_mes3_sol " + 
						"                   when t.mes = 8 and d.cuatrimestre = 2 then d.cuota_mes4_sol " + 
						"                   when t.mes = 9 and d.cuatrimestre = 3 then d.cuota_mes1_sol  " + 
						"                   when t.mes = 10 and d.cuatrimestre = 3 then d.cuota_mes2_sol " + 
						"                   when t.mes = 11 and d.cuatrimestre = 3 then d.cuota_mes3_sol " + 
						"                   when t.mes = 12 and d.cuatrimestre = 3 then d.cuota_mes4_sol " + 
						"                   end ) solicitado, " + 
						"                   sum(case  " + 
						"                   when t.mes = 1 and d.cuatrimestre = 1 then d.cuota_mes1_apr " + 
						"                   when t.mes = 2 and d.cuatrimestre = 1 then d.cuota_mes2_apr " + 
						"                   when t.mes = 3 and d.cuatrimestre = 1 then d.cuota_mes3_apr " + 
						"                   when t.mes = 4 and d.cuatrimestre = 1 then d.cuota_mes4_apr " + 
						"                   when t.mes = 5 and d.cuatrimestre = 2 then d.cuota_mes1_apr " + 
						"                   when t.mes = 6 and d.cuatrimestre = 2 then d.cuota_mes2_apr " + 
						"                   when t.mes = 7 and d.cuatrimestre = 2 then d.cuota_mes3_apr " + 
						"                   when t.mes = 8 and d.cuatrimestre = 2 then d.cuota_mes4_apr " + 
						"                   when t.mes = 9 and d.cuatrimestre = 3 then d.cuota_mes1_apr " + 
						"                   when t.mes = 10 and d.cuatrimestre = 3 then d.cuota_mes2_apr " + 
						"                   when t.mes = 11 and d.cuatrimestre = 3 then d.cuota_mes3_apr " + 
						"                   when t.mes = 12 and d.cuatrimestre = 3 then d.cuota_mes4_apr " + 
						"                   end ) aprobado " + 
						"                   FROM sicoinprod.EG_FINANCIERO_DETALLE_4 D,  " + 
						"                   sicoinprod.eg_financiero_hoja_4 H1, dashboard.tiempo t  " + 
						"                   WHERE  h1.ejercicio=2016   " + 
						"                   and h1.ejercicio = d.ejercicio " + 
						"                    and t.ejercicio = h1.ejercicio " + 
						"                    and t.dia = 1 " + 
						"                    AND h1.entidad = d.entidad  " + 
						"                    AND h1.unidad_ejecutora = d.unidad_ejecutora  " + 
						"                    AND h1.unidad_desconcentrada = d.unidad_desconcentrada  " + 
						"                    AND h1.no_cur = d.no_cur  " + 
						"                    AND H1.CLASE_REGISTRO IN ('RPG', 'PRG', 'RPGI')  " + 
						"                    AND H1.estado = 'APROBADO'  " + 
						"                    GROUP BY d.ejercicio,  " + 
						"                    t.mes, " + 
						"                    d.entidad,  " + 
						"                    d.unidad_ejecutora,  " + 
						"                    d.fuente "
						);	
				pstm.executeUpdate();
				pstm.close();
				
				CLogger.writeConsole("Insertando valores a MV_ANTICIPO");
				///Actualiza la vista de cuota
				pstm = conn.prepareStatement("INSERT INTO table dashboard.mv_anticipo "+
						"SELECT d.ejercicio, " + 
						" 		t.mes,   " + 
						"        d.entidad,   " + 
						"        d.unidad_ejecutora,   " + 
						"        d.fuente,   " + 
						"        sum(d.cuota_fondo_rotativo_apr + d.cuota_fideicomisos_apr + d.CUOTA_CONVENIOS_APR + d.cuota_contratos_apr + d.cuota_otros_apr + d.cuota_paa_apr) anticipo   " + 
						"	    FROM  sicoinprod.eg_anticipo_hoja h,   " + 
						"	          sicoinprod.eg_anticipo_detalle d, " + 
						"	          dashboard.tiempo t   " + 
						"	    WHERE h.ejercicio = d.ejercicio   " + 
						"	    AND h.ejercicio= year(current_date()) " + 
						"	    and t.ejercicio = h.ejercicio " + 
						"	    and t.dia = 1 " + 
						"	    and d.cuatrimestre = ceil(t.mes/4) " + 
						"	    and d.mes = t.mes-((d.cuatrimestre-1)*4) " + 
						"	    AND   h.entidad = d.entidad   " + 
						"	    AND   h.unidad_ejecutora = d.unidad_ejecutora   " + 
						"	    AND   h.no_cur = d.no_cur   " + 
						"	    AND   h.estado = 'APROBADO'   " + 
						"	    AND   h.clase_registro IN ('PRG','RPG')   " + 
						"	    group by d.ejercicio, t.mes, d.entidad, d.unidad_ejecutora, d.fuente"
						);	
				pstm.executeUpdate();
				pstm.close();
				
				CLogger.writeConsole("Insertando valores a MV_VIGENTE");
				//Actualiza la vista de mv_vigente
				pstm = conn.prepareStatement("INSERT INTO TABLE dashboard.mv_vigente select p.ejercicio, t.mes, p.entidad, p.unidad_ejecutora, p.programa, p.subprograma, p.proyecto, p.actividad, p.obra, p.fuente,  " + 
						"(p.renglon-p.renglon%100) grupo, gg.nombre grupo_nombre, (p.renglon-p.renglon%10) subgrupo, sg.nombre subgrupo_nombre,  " + 
						"p.renglon, r.nombre renglon_nombre, p.geografico, p.asignado,   " + 
						"						case   " + 
						"						               when t.mes=1 then vigente_1   " + 
						"						               when t.mes=2 then vigente_2   " + 
						"						               when t.mes=3 then vigente_3   " + 
						"						               when t.mes=4 then vigente_4   " + 
						"						               when t.mes=5 then vigente_5   " + 
						"						               when t.mes=6 then vigente_6   " + 
						"						               when t.mes=7 then vigente_7   " + 
						"						               when t.mes=8 then vigente_8   " + 
						"						               when t.mes=9 then vigente_9   " + 
						"						               when t.mes=10 then vigente_10   " + 
						"						               when t.mes=11 then vigente_11   " + 
						"						               when t.mes=12 then vigente_12   " + 
						"						end AS vigente   " + 
						"						from sicoinprod.vw_partidas p , dashboard.tiempo t, sicoinprod.cp_grupos_gasto gg, " + 
						"						sicoinprod.cp_objetos_gasto sg, sicoinprod.cp_objetos_gasto r   " + 
						"						where p.ejercicio= year(current_date())   " + 
						"						and p.ejercicio=t.ejercicio    " + 
						"						and t.dia=1 " + 
						"						and gg.grupo_gasto = (p.renglon - p.renglon%100) " + 
						"						and gg.ejercicio = p.ejercicio  " + 
						"						and sg.ejercicio = p.ejercicio " + 
						"						and sg.renglon = (p.renglon - p.renglon%10) " + 
						"						and r.ejercicio = p.ejercicio " + 
						"						and r.renglon = p.renglon");
				pstm.executeUpdate();
				pstm.close();
				
				CLogger.writeConsole("Insertando valores a MV_EJECUCION_PRESUPUESTARIA");
				//Actualiza la vista de mv_ejecucion_presupuestaria
				pstm = conn.prepareStatement("INSERT INTO TABLE dashboard.mv_ejecucion_presupuestaria  "+
						"select    " + 
						"												year(current_date()) ejercicio,    " + 
						"												t1.entidad,    " + 
						"												t1.unidad_ejecutora,    " + 
						"												t1.programa,    " + 
						"												t1.subprograma,    " + 
						"												t1.proyecto,    " + 
						"												t1.actividad,    " + 
						"												t1.obra,    " + 
						"												t1.mes, t1.fuente, t1.grupo, t1.grupo_nombre, t1.subgrupo, t1.subgrupo_nombre, t1.renglon, t1.renglon_nombre,     " + 
						"																		 t1.ano_1, t1.ano_2, t1.ano_3, t1.ano_4, t1.ano_5, t1.ano_actual, t1.asignado, t1.vigente, a.anticipo anticipo_cuota, c.solicitado solicitado_cuota,      " + 
						"																		 c.aprobado aprobado_cuota       " + 
						"																		 from (      " + 
						"																		 	select nvl(g.mes,v.mes) mes,       " + 
						"																		 	nvl(g.entidad, v.entidad) entidad,      " + 
						"																		 	nvl(g.unidad_ejecutora, v.unidad_ejecutora) unidad_ejecutora,      " + 
						"																		 	nvl(g.programa, v.programa) programa,      " + 
						"																		 	nvl(g.subprograma, v.subprograma) subprograma,      " + 
						"																		 	nvl(g.proyecto, v.proyecto) proyecto,      " + 
						"																		 	nvl(g.actividad, v.actividad) actividad,      " + 
						"																		 	nvl(g.obra, v.obra) obra,      " + 
						"																		 	nvl(g.fuente, v.fuente) fuente,      " + 
						"																		 	nvl(g.grupo, v.grupo) grupo,      " + 
						"																		 	nvl(g.grupo_nombre, v.grupo_nombre) grupo_nombre,      " + 
						"																		 	nvl(g.subgrupo, v.subgrupo) subgrupo,      " + 
						"																		 	nvl(g.subgrupo_nombre, v.subgrupo_nombre) subgrupo_nombre,      " + 
						"																		 	nvl(g.renglon, v.renglon) renglon,      " + 
						"																		 	nvl(g.renglon_nombre, v.renglon_nombre) renglon_nombre,      " + 
						"																			g.ano_1 ano_1, g.ano_2 ano_2, g.ano_3 ano_3, g.ano_4 ano_4, g.ano_5 ano_5, g.ano_actual ano_actual,      " + 
						"																		 	v.asignado asignado, v.vigente vigente      " + 
						"																		 	from (" + 
						"																		 		select g1.mes, g1.entidad, g1.unidad_ejecutora, g1.programa, g1.subprograma, g1.proyecto, g1.actividad, g1.obra, g1.fuente," + 
						"																		 		g1.grupo, g1.grupo_nombre, g1.subgrupo, g1.subgrupo_nombre, g1.renglon, g1.renglon_nombre," + 
						"																		 		sum(g1.ano_1) ano_1, sum(g1.ano_2) ano_2, sum(g1.ano_3) ano_3, sum(g1.ano_4) ano_4, sum(g1.ano_5) ano_5," + 
						"																		 		sum(g1.ano_actual) ano_actual" + 
						"																		 		from dashboard.mv_gasto g1" + 
						"																		 		group by g1.mes, g1.entidad, g1.unidad_ejecutora, g1.programa, g1.subprograma, g1.proyecto, g1.actividad, g1.obra, g1.fuente, " + 
						"																		 		g1.grupo, g1.grupo_nombre, g1.subgrupo, g1.subgrupo_nombre, g1.renglon, g1.renglon_nombre" + 
						"																		 	) g full outer join (	select mes, entidad, unidad_ejecutora, programa, subprograma, proyecto, actividad, obra, grupo, grupo_nombre, " + 
						"																									 subgrupo, subgrupo_nombre, renglon, renglon_nombre, fuente, sum(asignado) asignado, sum(vigente) vigente " + 
						"																										from dashboard.mv_vigente " + 
						"																										group by mes, entidad, unidad_ejecutora, programa, subprograma, proyecto, actividad, obra, grupo, grupo_nombre, subgrupo, subgrupo_nombre, renglon, renglon_nombre, fuente " + 
						"																									) v      " + 
						"																		 	on (      " + 
						"																		 		g.entidad = v.entidad      " + 
						"																		 		and g.unidad_ejecutora = v.unidad_ejecutora      " + 
						"																		 		and g.programa = v.programa      " + 
						"																		 		and g.subprograma = v.subprograma      " + 
						"																		 		and g.proyecto = v.proyecto      " + 
						"																		 		and g.actividad = v.actividad      " + 
						"																		 		and g.obra = v.obra      " + 
						"																		 		and g.renglon = v.renglon      " + 
						"																		 		and g.fuente = v.fuente      " + 
						"																		 		and g.mes = v.mes      " + 
						"																		 	)      " + 
						"																		 ) t1    " + 
						"																		 left outer join dashboard.mv_cuota c   " + 
						"																		 on (   " + 
						"																		 	c.entidad = t1.entidad      " + 
						"																		 	and c.unidad_ejecutora = t1.unidad_ejecutora      " + 
						"																		 	and c.fuente = t1.fuente      " + 
						"																		 	and c.mes = t1.mes   " + 
						"																		 )   " + 
						"																		 left outer join dashboard.mv_anticipo a  " + 
						"																		 on(  " + 
						"																		 	a.entidad = t1.entidad  " + 
						"																		 	and a.unidad_ejecutora = t1.unidad_ejecutora  " + 
						"																		 	and a.fuente = t1.fuente  " + 
						"																		 	and a.mes = t1.mes  " + 
						"																		 ) " );
				pstm.executeUpdate();
				pstm.close();
				
				CLogger.writeConsole("Insertando valores a MV_EJECUCION_PRESUPUESTARIA_GEOGRAFICO");
				//Actualiza la vista de mv_ejecucion_presupuestaria
				pstm = conn.prepareStatement("INSERT INTO TABLE dashboard.mv_ejecucion_presupuestaria_geografico "+
						"select " + 
						"year(current_date()) ejercicio, nvl(g.mes, v.mes),  " + 
						"nvl(g.entidad, v.entidad) entidad,  " + 
						"nvl(g.unidad_ejecutora, v.unidad_ejecutora) unidad_ejecutora,  " + 
						"nvl(g.programa, v.programa) programa,  " + 
						"nvl(g.subprograma, v.subprograma) subprograma,  " + 
						"nvl(g.proyecto, v.proyecto) proyecto,  " + 
						"nvl(g.actividad, v.actividad) actividad,  " + 
						"nvl(g.obra, v.obra) obra,  " + 
						"nvl(g.fuente, v.fuente) fuente,     " + 
						"nvl(g.grupo, v.grupo) grupo,  " + 
						"nvl(g.subgrupo, v.subgrupo) subgrupo,  " + 
						"nvl(g.renglon, v.renglon) renglon,  " + 
						"nvl(g.geografico, v.geografico) geografico,   " + 
						"sum(g.ano_actual) ano_actual,  " + 
						"sum(v.asignado) asignado, sum(v.vigente) vigente     " + 
						"from dashboard.mv_vigente v full outer join dashboard.mv_gasto g  " + 
						"on( g.mes = v.mes   " + 
						"and g.entidad = v.entidad  " + 
						"and g.unidad_ejecutora = v.unidad_ejecutora  " + 
						"and g.programa = v.programa  " + 
						"and g.subprograma = v.subprograma  " + 
						"and g.proyecto = v.proyecto  " + 
						"and g.actividad = v.actividad  " + 
						"and g.obra = v.obra  " + 
						"and g.mes = v.mes  " + 
						"and g.fuente = v.fuente  " + 
						"and g.grupo = v.grupo  " + 
						"and g.subgrupo = v.subgrupo  " + 
						"and g.renglon = v.renglon  " + 
						"and g.geografico = v.geografico )  " + 
						"group by year(current_date()), g.mes, v.mes, g.entidad, v.entidad, " + 
						"g.unidad_ejecutora, v.unidad_ejecutora, g.programa, v.programa, g.subprograma, v.subprograma, " + 
						"g.proyecto, v.proyecto, g.actividad, v.actividad, g.obra, v.obra, g.fuente, v.fuente, " + 
						"g.grupo, v.grupo, g.subgrupo, v.subgrupo, g.renglon, v.renglon,  g.geografico, v.geografico " );
				pstm.executeUpdate();
				pstm.close();
				
				boolean bconn =  CMemSQL.connect();
				CLogger.writeConsole("Cargando datos a cache de MV_EJECUCION_PRESUPUESTARIA");
				if(bconn){
					ret = true;
					int rows = 0;
					int rows_total=0;
					boolean first=true;
					PreparedStatement pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_ejecucion_presupuestaria(ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, " + 
							"proyecto, actividad, obra, renglon, renglon_nombre, subgrupo, subgrupo_nombre, grupo," + 
							"grupo_nombre, fuente, ano_1, ano_2, ano_3, ano_4, ano_5, ano_actual, solicitado, aprobado, anticipo, asignado, vigente) "
							+ "values (?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?) ");
					for(int i=1; i<13; i++){
						pstm = conn.prepareStatement("SELECT * FROM dashboard.mv_ejecucion_presupuestaria where mes = ?");
						pstm.setInt(1, i);
						pstm.setFetchSize(10000);
						ResultSet rs = pstm.executeQuery();
						while(rs!=null && rs.next()){
							if(first){
								PreparedStatement pstm2 = CMemSQL.getConnection().prepareStatement("delete from mv_ejecucion_presupuestaria ");
								if (pstm2.executeUpdate()>0)
									CLogger.writeConsole("Registros eliminados");
								else
									CLogger.writeConsole("Sin registros para eliminar");	
								pstm2.close();
								first=false;
							}
							pstm1.setInt(1, rs.getInt("ejercicio"));
							pstm1.setInt(2, rs.getInt("mes"));
							pstm1.setInt(3, rs.getInt("entidad"));
							pstm1.setInt(4, rs.getInt("unidad_ejecutora"));
							pstm1.setInt(5, rs.getInt("programa"));
							pstm1.setInt(6, rs.getInt("subprograma"));
							pstm1.setInt(7, rs.getInt("proyecto"));
							pstm1.setInt(8, rs.getInt("actividad"));
							pstm1.setInt(9, rs.getInt("obra"));
							pstm1.setInt(10, rs.getInt("renglon"));
							pstm1.setString(11, rs.getString("renglon_nombre"));
							pstm1.setInt(12, rs.getInt("subgrupo"));
							pstm1.setString(13, rs.getString("subgrupo_nombre"));
							pstm1.setInt(14, rs.getInt("grupo"));
							pstm1.setString(15, rs.getString("grupo_nombre"));
							pstm1.setInt(16, rs.getInt("fuente"));
							pstm1.setDouble(17, rs.getDouble("ano_1"));
							pstm1.setDouble(18, rs.getDouble("ano_2"));
							pstm1.setDouble(19, rs.getDouble("ano_3"));
							pstm1.setDouble(20, rs.getDouble("ano_4"));
							pstm1.setDouble(21, rs.getDouble("ano_5"));
							pstm1.setDouble(22, rs.getDouble("ano_actual"));
							Double solicitado_cuota=rs.getDouble("solicitado_cuota");
							if(!rs.wasNull())
								pstm1.setDouble(23, solicitado_cuota);
							else
								pstm1.setObject(23, null);
							Double aprobado_cuota=rs.getDouble("aprobado_cuota");
							if(!rs.wasNull())
								pstm1.setDouble(24, aprobado_cuota);
							else
								pstm1.setObject(24, null);
							Double anticipo_cuota=rs.getDouble("anticipo_cuota");
							if(!rs.wasNull())
								pstm1.setDouble(25, anticipo_cuota);
							else
								pstm1.setObject(25, null);
							pstm1.setDouble(26, rs.getDouble("asignado"));
							pstm1.setDouble(27, rs.getDouble("vigente"));
							pstm1.addBatch();
							rows++;
							if((rows % 10000) == 0)
								pstm1.executeBatch();
						}
						CLogger.writeConsole("Records escritos: "+rows+" - mes: "+i);
						pstm1.executeBatch();
						rows_total += rows;
						rows=0;
						rs.close();
						pstm.close();
					}
					
					CLogger.writeConsole("Records escritos Totales: "+rows_total);
					pstm1.close();
					
					CLogger.writeConsole("Cargando datos a cache de MV_EJECUCION_PRESUPUESTARIA_GEOGRAFICO");
					ret = true;
					rows = 0;
					first=true;
					pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_ejecucion_presupuestaria_geografico(ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, " + 
							"proyecto, actividad, obra, renglon, subgrupo, grupo," + 
							"fuente, geografico, ano_actual, asignado, vigente) "
							+ "values (?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?,?,?) ");
					for(int i=1; i<13; i++){
						pstm = conn.prepareStatement("SELECT * FROM dashboard.mv_ejecucion_presupuestaria_geografico where mes = ?");
						pstm.setInt(1, i);
						pstm.setFetchSize(10000);
						ResultSet rs = pstm.executeQuery();
						while(rs!=null && rs.next()){
							if(first){
								PreparedStatement pstm2 = CMemSQL.getConnection().prepareStatement("delete from mv_ejecucion_presupuestaria_geografico ");
								if (pstm2.executeUpdate()>0)
									CLogger.writeConsole("Registros eliminados");
								else
									CLogger.writeConsole("Sin registros para eliminar");	
								pstm2.close();
								first=false;
							}
							pstm1.setInt(1, rs.getInt("ejercicio"));
							pstm1.setInt(2, rs.getInt("mes"));
							pstm1.setInt(3, rs.getInt("entidad"));
							pstm1.setInt(4, rs.getInt("unidad_ejecutora"));
							pstm1.setInt(5, rs.getInt("programa"));
							pstm1.setInt(6, rs.getInt("subprograma"));
							pstm1.setInt(7, rs.getInt("proyecto"));
							pstm1.setInt(8, rs.getInt("actividad"));
							pstm1.setInt(9, rs.getInt("obra"));
							pstm1.setInt(10, rs.getInt("renglon"));
							pstm1.setInt(11, rs.getInt("subgrupo"));
							pstm1.setInt(12, rs.getInt("grupo"));
							pstm1.setInt(13, rs.getInt("fuente"));
							pstm1.setInt(14, rs.getInt("geografico"));
							pstm1.setDouble(15, rs.getDouble("ano_actual"));
							pstm1.setDouble(16, rs.getDouble("asignado"));
							pstm1.setDouble(17, rs.getDouble("vigente"));
							pstm1.addBatch();
							rows++;
							if((rows % 10000) == 0)
								pstm1.executeBatch();
						}
						CLogger.writeConsole("Records escritos: "+rows+" - mes: "+i);
						pstm1.executeBatch();
						rows_total += rows;
						rows=0;
						rs.close();
						pstm.close();
					}
					
					CLogger.writeConsole("Records escritos Totales: "+rows_total);
					pstm1.close();
					
					CLogger.writeConsole("Cargando datos a cache de MV_ESTRUCTURA");
					ret = true;
					rows = 0;
					first=true;
					pstm1 = CMemSQL.getConnection().prepareStatement("Insert INTO mv_estructura(ejercicio, entidad, entidad_nombre, sigla, unidad_ejecutora, unidad_ejecutora_nombre, "
							+ "programa, programa_nombre, subprograma, subprograma_nombre, proyecto, proyecto_nombre, "
							+ "actividad, obra, actividad_obra_nombre) "
							+ "values (?,?,?,?,?,?,?,?,?,?,"
							+ "?,?,?,?,?) ");
					pstm = conn.prepareStatement("SELECT * FROM dashboard.mv_estructura");
					pstm.setFetchSize(10000);
					ResultSet rs = pstm.executeQuery();
					while(rs!=null && rs.next()){
						if(first){
							PreparedStatement pstm2 = CMemSQL.getConnection().prepareStatement("delete from mv_estructura ");
							if (pstm2.executeUpdate()>0)
								CLogger.writeConsole("Registros eliminados");
							else
								CLogger.writeConsole("Sin registros para eliminar");	
							pstm2.close();
							first=false;
						}
						pstm1.setInt(1, rs.getInt("ejercicio"));
						pstm1.setInt(2, rs.getInt("entidad"));
						pstm1.setString(3, rs.getString("entidad_nombre"));
						pstm1.setString(4, rs.getString("sigla"));
						pstm1.setInt(5, rs.getInt("unidad_ejecutora"));
						pstm1.setString(6, rs.getString("unidad_ejecutora_nombre"));
						pstm1.setInt(7, rs.getInt("programa"));
						pstm1.setString(8, rs.getString("programa_nombre"));
						pstm1.setInt(9, rs.getInt("subprograma"));
						pstm1.setString(10, rs.getString("subprograma_nombre"));
						pstm1.setInt(11, rs.getInt("proyecto"));
						pstm1.setString(12, rs.getString("proyecto_nombre"));
						pstm1.setInt(13, rs.getInt("actividad"));
						pstm1.setInt(14, rs.getInt("obra"));
						pstm1.setString(15, rs.getString("actividad_obra_nombre"));
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
			CLogger.writeFullConsole("Error 1: CEjecucionPresupuestaria.class", e);
		}
		finally{
			CMemSQL.close();
		}
		return ret;
	}
}
