package org.fao.openforis.collect.migrate;

import org.apache.commons.lang3.StringUtils;
import org.openforis.collect.model.CollectSurvey;
import org.openforis.collect.persistence.xml.DataHandler;
import org.openforis.idm.model.Attribute;
import org.openforis.idm.model.CodeAttribute;
import org.openforis.idm.model.DateAttribute;
import org.openforis.idm.model.Entity;
import org.openforis.idm.model.Field;
import org.openforis.idm.model.Node;
import org.openforis.idm.model.NumberAttribute;
import org.openforis.idm.model.TimeAttribute;
import org.xml.sax.Attributes;

/**
 * 
 * @author G. Miceli
 *
 */
public class NaformaDataHandler extends DataHandler {

	private boolean inQualifierElement;
	
	public NaformaDataHandler(CollectSurvey survey) {
		super(survey);
	}
	
	@Override
	public void startChildNode(String localName, Attributes attributes) {
		if ( localName.equals("type_other") ) {
			String name = localName.replace("_other", "");
			Entity parent = (Entity) getNode();
			int count = parent.getCount(name);
			Node<?> node = parent.get(name, count-1);
			if ( node == null ) {
				warn("Unknown node'"+name+"' in "+getPath());
				pushIgnore();
			} else if ( node instanceof CodeAttribute ){
				inQualifierElement = true;
				setNode(node);
			} else {
				fail("Related qualifier node is not a CodeAttribute at "+getPath());
			}
		} else if ( localName.equals("unit_other") && getNode().getName().equals("energy_source") ) {
			super.startChildNode("monthly_consumption_unit", attributes);
		} else {
			super.startChildNode(localName, attributes);
		}
	}
	@Override
	protected void setField(Field<?> fld) {
		String str = getXmlValue();
		Character symbol = mapSymbol(str);
		if ( symbol == null ) {
			Attribute<?, ?> attr = (Attribute<?,?>) getNode();
			if ( attr instanceof NumberAttribute ) {
				str = str == null ? null : str.replace(",", "");
				setXmlValue(str);
			}
			super.setField(fld);
		} else {
			fld.setSymbol(symbol);
		}
	}

	private Character mapSymbol(String value) {
		if ( "---".equals(value) || "--".equals(value) ||  "_".equals(value) ||
		  	   "-".equals(value) || "=".equals(value) ||
			"0-".equals(value) || "-+".equals(value) || 
			"NA".equals(value) || "N/A".equals(value) || "na".equals(value) || 
			"`-".equals(value) || "*-".equals(value) || "-*".equals(value) ) {
			return '-';
		} else if ( "*".equals(value) || "**".equals(value) ) {
			return '*';
		} else {
			return null;
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void setSingleElementValue(Attribute attr) {
		String str = getXmlValue();
		Character symbol;
		if ( (attr instanceof TimeAttribute || attr instanceof DateAttribute)
				&& StringUtils.isNotBlank(str) && str.contains("*") ) {
			symbol = '*';
			str = "";
		} else if ( attr instanceof TimeAttribute && StringUtils.isNotBlank(str) && str.contains("-") ) {
				symbol = '-';
				str = "";
		} else {
			symbol = mapSymbol(str);
		}
		
		if ( inQualifierElement ) {
			attr.getField(1).setValue(str);
			inQualifierElement = false;
		} else {
			if ( symbol == null ) {
				if ( attr instanceof NumberAttribute ) {
					str = str == null ? null : str.replace(",", "");
					setXmlValue(str);
				}
				super.setSingleElementValue(attr);
			} else {
				for (int i=0; i< attr.getFieldCount();i++) {
					Field<?> fld = attr.getField(i);
					fld.setSymbol(symbol);
				}
			}
		}
	}
}
