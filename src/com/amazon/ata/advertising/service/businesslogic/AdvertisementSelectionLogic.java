package com.amazon.ata.advertising.service.businesslogic;

import com.amazon.ata.advertising.service.dao.ReadableDao;
import com.amazon.ata.advertising.service.model.*;
import com.amazon.ata.advertising.service.targeting.TargetingEvaluator;
import com.amazon.ata.advertising.service.targeting.TargetingGroup;

import com.amazon.ata.advertising.service.targeting.predicate.TargetingPredicate;
import com.amazon.ata.advertising.service.targeting.predicate.TargetingPredicateResult;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;

/**
 * This class is responsible for picking the advertisement to be rendered.
 */
public class AdvertisementSelectionLogic {

    private static final Logger LOG = LogManager.getLogger(AdvertisementSelectionLogic.class);

    private final ReadableDao<String, List<AdvertisementContent>> contentDao;
    private final ReadableDao<String, List<TargetingGroup>> targetingGroupDao;
    private Random random = new Random();

    /**
     * Constructor for AdvertisementSelectionLogic.
     * @param contentDao Source of advertising content.
     * @param targetingGroupDao Source of targeting groups for each advertising content.
     */
    @Inject
    public AdvertisementSelectionLogic(ReadableDao<String, List<AdvertisementContent>> contentDao,
                                       ReadableDao<String, List<TargetingGroup>> targetingGroupDao) {
        this.contentDao = contentDao;
        this.targetingGroupDao = targetingGroupDao;
    }

    /**
     * Setter for Random class.
     * @param random generates random number used to select advertisements.
     */
    public void setRandom(Random random) {
        this.random = random;
    }

    /**
     * Gets all of the content and metadata for the marketplace and determines which content can be shown.  Returns the
     * eligible content with the highest click through rate.  If no advertisement is available or eligible, returns an
     * EmptyGeneratedAdvertisement.
     *
     * @param customerId - the customer to generate a custom advertisement for
     * @param marketplaceId - the id of the marketplace the advertisement will be rendered on
     * @return an advertisement customized for the customer id provided, or an empty advertisement if one could
     *     not be generated.
     */
    public GeneratedAdvertisement selectAdvertisement(String customerId, String marketplaceId) {
        GeneratedAdvertisement generatedAdvertisement = new EmptyGeneratedAdvertisement();
        if (StringUtils.isEmpty(marketplaceId)) {
            LOG.warn("MarketplaceId cannot be null or empty. Returning empty ad.");
        } else {
            final List<AdvertisementContent> contents = contentDao.get(marketplaceId);

            if (CollectionUtils.isNotEmpty(contents)) {
                List<AdvertisementContent> eligibleAdvertisements =
                        filterEligibleAdvertisements(contents, customerId, marketplaceId);

                if (CollectionUtils.isNotEmpty(eligibleAdvertisements)) {
                    //Ad that the customer is eligible for with the highest click through rate.
                    SortedMap<Double, AdvertisementContent> advertisementContentTreeMap =
                                                        filterCTR(eligibleAdvertisements, customerId, marketplaceId);

                    AdvertisementContent advertisementContentCTR = advertisementContentTreeMap
                            .get(advertisementContentTreeMap.firstKey());

                    generatedAdvertisement = new GeneratedAdvertisement(advertisementContentCTR);
                }
            }
        }
        return generatedAdvertisement;
    }

    private List<AdvertisementContent> filterEligibleAdvertisements(List<AdvertisementContent> contents, String customerId, String marketplaceId) {
        final TargetingEvaluator targetingEvaluator =
                new TargetingEvaluator(new RequestContext(customerId, marketplaceId));
        return contents.stream()
                //Filter: at least one targeting group for customer in ad content
                .filter(advertisementContent -> targetingGroupDao.get(advertisementContent.getContentId())
                        .stream()
                        .map(targetingEvaluator::evaluate)
                        .anyMatch(TargetingPredicateResult::isTrue))
                .collect(Collectors.toList());

    }

    private SortedMap<Double, AdvertisementContent> filterCTR(List<AdvertisementContent> eligibleAdvertisements,
                                                            String customerId, String marketplaceId) {

        final TargetingEvaluator targetingEvaluator =
                new TargetingEvaluator(new RequestContext(customerId, marketplaceId));
        SortedMap<Double, AdvertisementContent> advertisementContentTreeMap =
                new TreeMap<>(Comparator.reverseOrder());

        for (AdvertisementContent advertisementContent : eligibleAdvertisements) {

            SortedMap<Double, AdvertisementContent> potentialClickThru =
                    new TreeMap<>(Comparator.reverseOrder());
            for (TargetingGroup targetingGroup : targetingGroupDao.get(advertisementContent.getContentId())) {
                //if the targetingGroup evaluates to true compare against others in the group
                boolean isEligibleCRT = targetingEvaluator.evaluate(targetingGroup).equals(TargetingPredicateResult.TRUE);
                if (isEligibleCRT) {
                    potentialClickThru.put(targetingGroup.getClickThroughRate(), advertisementContent);
                }
            }
            //select the highest CTR  for the ad and add to the tree.
            Double key = potentialClickThru.firstKey();
            advertisementContentTreeMap.put(key, potentialClickThru.get(key));
        }
        return advertisementContentTreeMap;
    }
}
