<!-- $%BEGINLICENSE%$
 Copyright (c) 2008, Oracle and/or its affiliates. All rights reserved.

 This program is free software; you can redistribute it and/or
 modify it under the terms of the GNU General Public License as
 published by the Free Software Foundation; version 2 of the
 License.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 02110-1301  USA

 $%ENDLICENSE%$ -->
<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <!--
        jsled@mysql.com, 2008-06-17.  Convert mysql-lb's gtester output into
        junit-style report xml for hudson to consume.
    -->
    <xsl:output method="xml" cdata-section-elements="system-out system-err failure" indent="yes"/>
    <xsl:template match="/gtester/testbinary/testcase">
        <testcase>
            <xsl:attribute name="time">
                <xsl:value-of select="./duration/text()"/>
            </xsl:attribute>
            <xsl:attribute name="classname">
                <xsl:value-of select="concat('com.mysql.proxy.', translate(../@path, '/', '_'))"/>
            </xsl:attribute>
            <!-- translate some names (above and below) to try and get
                 somewhat closer to java-style package names, which hudson
                 keys off of for the ui -->
            <xsl:attribute name="name">
                <xsl:value-of select="translate(@path, '/-', '_')"/>
            </xsl:attribute>
            <xsl:if test="./status[@result='failed']">
                <failure>
                    <xsl:apply-templates select="./error"/>
                </failure>
            </xsl:if>
        </testcase><xsl:text>
</xsl:text>
    </xsl:template>
    <!-- what a convoluted way to say "or". :( -->
    <xsl:template match="/gtester/testbinary/testcase/child::*[self::error or self::message]">
        <xsl:value-of select="./text()"/>
        <xsl:text>
</xsl:text>
    </xsl:template>
    <xsl:template match="text()"/>
    <xsl:template match="/">
        <testsuite errors="0" failures="0" name="com.mysql.proxy.testsuite.All">
            <xsl:attribute name="time">
                <xsl:value-of select="./gtester/testbinary/duration"/>
            </xsl:attribute>
            <xsl:attribute name="tests">
                <xsl:value-of select="count(/gtester/testbinary/testcase)"/>
            </xsl:attribute>
            <xsl:attribute name="failures">
                <xsl:value-of select="count(/gtester/testbinary/testcase/status[@result=&quot;failed&quot;])"/>
            </xsl:attribute>
<xsl:text>
</xsl:text>
            <xsl:apply-templates select="/gtester/testbinary/testcase"/>
            <system-out><xsl:text>
</xsl:text>
                <xsl:apply-templates select="/gtester/testbinary/testcase/message"/>
            </system-out>
            <system-err><xsl:text>
</xsl:text>
                <xsl:apply-templates select="/gtester/testbinary/testcase/error"/>
            </system-err>
        </testsuite>
    </xsl:template>
</xsl:stylesheet>
