It is correct that we do not create additional hubs in BDV. Hubs represent business keys and should exist only in RDV. BDV does not redefine business keys, it only adds business meaning on top of them.

However, this does not mean that RDV and BDV are “mixed” in an uncontrolled way. They are still logically separate layers, but they can reference the same hubs and links. In practice, BDV builds on top of RDV structures rather than duplicating them.

For satellites and links:

RDV contains raw satellites and links, strictly reflecting source data

BDV introduces business satellites, PIT tables, and sometimes derived relationships

Regarding the point about satellites with 1:1 fields, this is exactly why I raised that question earlier. If a satellite does not add history, business logic, or new meaning, then it may not belong in BDV and should be reconsidered.

So the goal is not to physically duplicate everything between RDV and BDV, but to clearly separate responsibilities:

RDV = raw, auditable history

BDV = business interpretation and rules applied on top

I agree that based on this, we should revisit parts of the current BDV design, but this should lead to a cleaner and more intentional model rather than just a smaller one.

Let me know if you want to walk through a concrete example together.

Best regards,
