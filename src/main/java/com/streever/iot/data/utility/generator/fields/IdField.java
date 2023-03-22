package com.streever.iot.data.utility.generator.fields;

import com.streever.iot.data.utility.generator.RelationshipType;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.util.List;
import java.util.UUID;

public class IdField extends StringField {
    @JsonIgnore
    /*
    For SOURCE and TRANSACTIONAL, this would be null.
    For REFERENCE, this would be the Reference Schema id field
     */
    private IdField relationshipId;

    private Boolean compound = Boolean.TRUE;
//    @JsonIgnore
//    private Integer maxValue = null;
//    @JsonIgnore
//    private DigestUtils dg = null;

//    private Range<Integer> range = null; //new Range(0, Integer.MAX_VALUE);
//    private Boolean hash = Boolean.FALSE;
    /*
    Algorithm used to digest the id value when 'hash' is 'true'.
     */
//    private String messageDigest = "md5";

    /*
    Ordering of fields is important to control how they are loaded.
    The 'type' is last, so we can set/fix previous values like range, hash, digest, etc..
     */
    private RelationshipType idType = null;

    public Boolean getCompound() {
        return compound;
    }

    public void setCompound(Boolean compound) {
        this.compound = compound;
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.STRING;
    }

    public RelationshipType getIdType() {
        return idType;
    }

    public void setIdType(RelationshipType type) {
        this.idType = type;
        switch (this.idType) {
            case SOURCE:
                // We should have a Range element.
                // TODO: Assert that a range exists.

                break;
//            case CHILD:
//                // The reference schema should not have a hash element.
//                // We don't need one here.
//                hash = null;
//                messageDigest = null;
            case TRANSACTIONAL:
                // Just make the value a UUID for Uniqueness.
                // range not needed for REFERENCE and TRANSACTIONAL
                setRange(null);
                break;
        }
    }

    public IdField getRelationshipId() {
        return relationshipId;
    }

    public void setRelationshipId(IdField relationshipId) {
        this.relationshipId = relationshipId;
    }

    private String getNextIdNum() {
        // Range needs to be set.
        assert getRange() != null : "range isn't set for id field type(getNextIdNum)" + getParent().getTitle();
        Integer nextId = getRange().getMin() + randomizer.nextInt(getDiff());
        // Pad Left with '0' to max potential length +2
        return StringUtils.leftPad(nextId.toString(), getRange().getMax().toString().length() + 2, "0");
    }

    @Override
    public String getNext() {
        // Default StringField Behavior
        String rtn = null;

        switch (idType) {
            case REFERENCE:
                rtn = this.getRelationshipId().getNext();
                break;
            case SOURCE:
                if (compound) {
                    StringBuilder sb = new StringBuilder();
                    sb.append(getParent().getTitle()).append("_");
                    sb.append(getNextIdNum());
                    if (isHash()) {
                        rtn = getDg().digestAsHex(sb.toString()).toUpperCase();
                    } else {
                        rtn = sb.toString();
                    }
                } else {
                    rtn = super.getNext();
                }
                break;
//            case PEER:
//                // Expect that we have set the parentId reference
//                assert relationship != null: "parentId isn't set for id field with type 'PEER'";
//                IdField[] idFields = relationship.getIdFields();
//                StringBuilder idsBldr = new StringBuilder();
//                for (IdField idField: idFields) {
//                    idsBldr.append(idField.getNext());
//                }
//                rtn = idsBldr.toString();
//                break;
            case TRANSACTIONAL:
                UUID uuid = UUID.randomUUID();
                rtn = uuid.toString();
                break;
            case LOOKUP:
                // TODO: Need to handle compound return
//                rtn = relationshipId.getNext();//.getLookUpIdValues()[0].toString();
                rtn = relationshipId.getParent().getLookUpIdValues()[0].toString();
                break;
        }
        setLast(rtn);
        return rtn;
    }

    @Override
    public Boolean validate(List<String> reasons) {
        Boolean rtn = super.validate(reasons);
        if (!getParent().isStatic() && relationshipId == null && compound && idType != RelationshipType.TRANSACTIONAL) {
            if (getDiff() <= 0) {
                rtn = Boolean.FALSE;
                reasons.add(getParent().getTitle() + ":" + getName() + " when id is 'compound', range diff must be greater than 0");
            }
        }
        return rtn;
    }
}
