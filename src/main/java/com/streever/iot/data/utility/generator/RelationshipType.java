package com.streever.iot.data.utility.generator;

public enum RelationshipType {
    /*
    Use to id this is the source of a relationship.
     */
    SOURCE,
    /*
    Need to include in build of parent.
    Link Childs parentId to the Parent.Id field
     */
    CHILD,
    /*
    Not included in build of Parent.
    Parent.Id field is Linked so the TRANSACTIONAL object can build the relationship ID based on the parent's id field.
     */
    TRANSACTIONAL,
    /*
    The lookup is static.
     */
    LOOKUP,
    REFERENCE;
}
