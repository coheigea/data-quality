// ============================================================================
//
// Copyright (C) 2006-2015 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.dataquality.semantic.classifier.impl;

import java.util.HashSet;
import java.util.Set;

import org.talend.dataquality.semantic.classifier.ISubCategory;
import org.talend.dataquality.semantic.classifier.ISubCategoryClassifier;
import org.talend.dataquality.semantic.filter.ISemanticFilter;
import org.talend.dataquality.semantic.validator.ISemanticValidator;

/**
 * Created by sizhaoliu on 20/03/15.
 */
public abstract class AbstractSubCategoryClassifier implements ISubCategoryClassifier {

    protected Set<ISubCategory> potentialSubCategories = new HashSet<>();

    @Override
    @Deprecated
    public Set<String> classify(String str) {
        Set<String> catSet = new HashSet<>();
        Set<ISubCategory> categories = classifyIntoCategories(str);
        for (ISubCategory iSubCategory : categories) {
            catSet.add(iSubCategory.getName());
        }
        return catSet;
    }

    @Override
    public Set<ISubCategory> classifyIntoCategories(String str) {
        Set<ISubCategory> catSet = new HashSet<>();
        for (ISubCategory classifier : potentialSubCategories) {
            ISemanticFilter filter = classifier.getFilter();

            if (filter != null) {
                if (!filter.isQualified(str)) {
                    continue;
                }
            }
            ISemanticValidator validator = classifier.getValidator();
            if (validator != null && validator.isValid(str)) {
                catSet.add(classifier);
            }
        }

        return catSet;

    }

    public Set<ISubCategory> getClassifiers() {
        return potentialSubCategories;
    }

    public void setClassifiers(Set<ISubCategory> classifiers) {
        this.potentialSubCategories = classifiers;
    }

}