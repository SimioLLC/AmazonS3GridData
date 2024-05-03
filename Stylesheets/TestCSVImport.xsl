<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    exclude-result-prefixes="xsl">
    <xsl:output method="xml" encoding="utf-8" />
    
    <xsl:variable name="newline" select="'&#10;'" />
    <xsl:variable name="comma" select="','" />
    <xsl:variable name="first-row" select="substring-before( concat( ., $newline ), $newline )" />
      
    <xsl:template match="/">    
         <data>
            <xsl:call-template name="write-line" />
        </data>
        <xsl:call-template name="output" />
    </xsl:template>  
      
   <xsl:template name="write-line">
        <xsl:param name="text" select="." />
        <xsl:variable name="this-row" select="substring-before( concat( $text, $newline ), $newline )" />
        <xsl:variable name="remaining-rows" select="substring-after( $text, $newline )" />
        
        <xsl:if test="$this-row != $first-row">          
            <xsl:if test="string-length($this-row) &gt; 1">
                <row>
                    <xsl:call-template name="write-item">
                        <xsl:with-param name="line" select="$this-row" />
                        <xsl:with-param name="element-names" select="$first-row" />
                    </xsl:call-template>
                </row>
            </xsl:if>
        </xsl:if>
        <xsl:if test="string-length( $remaining-rows ) &gt; 0">
            <xsl:call-template name="write-line">
                <xsl:with-param name="text" select="$remaining-rows" />
            </xsl:call-template>
        </xsl:if>
    </xsl:template>
    
    <xsl:template name="write-item">
        <xsl:param name="line"/>
        <xsl:param name="element-names"/>
        <xsl:variable name="element-name" select="substring-before( concat( $element-names, $comma ), $comma)" />
        <xsl:variable name="remaining-element-names" select="substring-after( $element-names, $comma )" />
        <xsl:variable name="this-item" select="substring-before( concat( $line, $comma ), $comma)" />
        <xsl:variable name="remaining-items" select="substring-after( $line, $comma )" />

        <xsl:element name="{$element-name}">
            <xsl:value-of select="$this-item" />
        </xsl:element>
        <xsl:if test="string-length( $remaining-items ) &gt; 0">
            <xsl:call-template name="write-item">
                <xsl:with-param name="line" select="$remaining-items" />
                <xsl:with-param name="element-names" select="$remaining-element-names" />
            </xsl:call-template>
        </xsl:if>
    </xsl:template>
    
    <xsl:template name="output" />        

    
</xsl:stylesheet>