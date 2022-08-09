package org.dizitart.no2.filters;

import com.fasterxml.jackson.databind.Module;
import org.dizitart.no2.Filter;

public final class MixInAnnotationsRegistrar {

	private MixInAnnotationsRegistrar() {}

	public static void registerMixInAnnotations(Module.SetupContext context) {
		context.setMixInAnnotations(Filter.class, FilterMixIn.class);
		context.setMixInAnnotations(AndFilter.class, AndFilterMixIn.class);
		context.setMixInAnnotations(OrFilter.class, OrFilterMixIn.class);
		context.setMixInAnnotations(EqualsFilter.class, EqualsFilterMixIn.class);
		context.setMixInAnnotations(PatchedEqualsFilter.class, EqualsFilterMixIn.class);
		context.setMixInAnnotations(NotFilter.class, NotFilterMixIn.class);
		context.setMixInAnnotations(InFilter.class, InFilterMixIn.class);
		context.setMixInAnnotations(NotInFilter.class, NotInFilterMixIn.class);
		context.setMixInAnnotations(GreaterThanFilter.class, ComparisonFilterMixIn.class);
		context.setMixInAnnotations(PatchedGreaterThanFilter.class, ComparisonFilterMixIn.class);
		context.setMixInAnnotations(GreaterEqualFilter.class, ComparisonFilterMixIn.class);
		context.setMixInAnnotations(PatchedGreaterEqualFilter.class, ComparisonFilterMixIn.class);
		context.setMixInAnnotations(LesserThanFilter.class, ComparisonFilterMixIn.class);
		context.setMixInAnnotations(PatchedLesserThanFilter.class, ComparisonFilterMixIn.class);
		context.setMixInAnnotations(LesserEqualFilter.class, ComparisonFilterMixIn.class);
		context.setMixInAnnotations(PatchedLesserEqualFilter.class, ComparisonFilterMixIn.class);
		context.setMixInAnnotations(RegexFilter.class, StringFilterMixIn.class);
		context.setMixInAnnotations(TextFilter.class, StringFilterMixIn.class);
		context.setMixInAnnotations(ElementMatchFilter.class, ElementMatchFilterMixIn.class);
	}

}
